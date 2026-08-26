// © Copyright 2026 Query Farm LLC - https://query.farm
//
// See vgi_table_in_out_vtab.h for the module's overall design. This file
// comment covers what didn't fit there: the specific xBestIndex/xFilter
// mechanics, and why transactions/projection/filter pushdown are each
// left out for v1 rather than half-implemented.
//
// xBestIndex requires an EQ constraint on EVERY hidden (argument) column -
// a table_in_out call with a missing argument can't be bound at all, the
// same way a plain SQL function call with a missing argument can't. This
// mirrors SQLite's own reference table-valued functions (json_each,
// generate_series): return SQLITE_CONSTRAINT from xBestIndex when a
// required hidden-column constraint isn't usable, telling the SQLite core
// this specific plan can't work - not a query-time error, a "try a
// different plan, and if none exists, report no query solution at
// prepare time" signal (sqlite.org/vtab.html's xBestIndex documentation).
// Every claimed argument constraint sets .omit=1 - unlike vgi_worker's
// WHERE-pushdown (which never omits, since a worker might silently ignore
// the pushdown hint and the *same column's real fetched value* still
// needs rechecking), a HIDDEN argument column has no independent fetched
// value to recheck against at all - the argument binding IS how the row
// was produced, not an optional narrowing of some other data.
//
// No idxStr/idxNum needed: argvIndex is always assigned in a fixed order
// (hidden column 0 -> argv[0], column 1 -> argv[1], ...), regardless of
// which physical aConstraint[] slot each came from - SQLite guarantees
// xFilter's argv[] follows argvIndex, not constraint-array order, so
// xFilter can read its input row directly with no decoding step, unlike
// vgi_worker's EncodeIdxStr/DecodeIdxStr (which exists there because that
// module's constraint set is genuinely variable per query, not "exactly
// one required binding per hidden column, always").
//
// No pushdown_filters, no projection_ids sent to init(): per this
// feature's design research (see the plan file's Milestone 9 notes,
// citing vgi's own DuckDB client), DuckDB's planner never sends
// table_filters to a table-in-out function's init at all - filtering
// happens locally over the fetched result the same way any other
// unclaimed WHERE constraint does. Projection pushdown is a real,
// available optimization for a worker that declares it
// (CatalogTableInOutFunction::projection_pushdown, decoded but not yet
// wired to InitRequest.projection_ids here) - skipped for v1 to keep the
// first working version simple, matching this driver's established "ship
// simple round trips first" precedent (ScalarFunctionCaller, TableWriter).
//
// No transactions: xBegin/xSync/xCommit/xRollback are all left null. This
// vtab never writes, so there's nothing to commit/roll back; and unlike
// vgi_worker's plain table scans (which participate in VGI's transaction
// coordination purely for read-consistency reasons - see
// connection_pool.h's file comment), a table_in_out call's per-row
// Exchange() already gets all the consistency one query needs from a
// single bind()/init() pair shared across every row in that query's
// cursor - there's no cross-statement state for a transaction wrapper to
// coordinate. A worker whose table_in_out function genuinely depends on
// transactional catalog state (e.g. reading a value another statement in
// the same explicit transaction just wrote) isn't supported by this
// v1 - a documented gap, not a silent one.
#include "vtab/vgi_table_in_out_vtab.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

#include "catalog/catalog_client.h"
#include "catalog/table_in_out_caller.h"
#include "sql_quote.h"
#include "types/type_mapping.h"
#include "vtab/connection_pool.h"

namespace vgi_sqlite {
namespace {

std::map<std::string, std::string> ParseModuleArgs(int argc, const char* const* argv) {
    std::map<std::string, std::string> args;
    for (int i = 3; i < argc; ++i) {
        std::string token(argv[i]);
        auto eq = token.find('=');
        if (eq == std::string::npos) continue;
        std::string key = token.substr(0, eq);
        std::string value = token.substr(eq + 1);
        auto trim = [](std::string& s) {
            size_t b = s.find_first_not_of(" \t\n");
            size_t e = s.find_last_not_of(" \t\n");
            s = (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
        };
        trim(key);
        trim(value);
        if (value.size() >= 2 && (value.front() == '\'' || value.front() == '"') &&
            value.back() == value.front()) {
            value = value.substr(1, value.size() - 2);
        }
        args[key] = value;
    }
    return args;
}

// One vgi_table_in_out table. Doesn't hold a connection long-term (same
// reasoning as VgiVtab in vgi_vtab.cpp) - input_schema/output_schema are
// resolved once at xConnect/xCreate via a throwaway bind (see
// ConnectImpl) and cached here; every real query gets its own fresh
// TableInOutCaller (and hence its own fresh bind+init+stream) per cursor.
struct VgiTableInOutVtab {
    sqlite3_vtab base;  // must be first member (sqlite3 vtab ABI)
    ConnectionPool* pool = nullptr;
    std::string location;
    std::string catalog_name;
    std::string schema_name;   // where the FUNCTION is registered
    std::string function_name;
    std::shared_ptr<arrow::Schema> input_schema;   // declared arg names+types; HIDDEN column order
    std::shared_ptr<arrow::Schema> output_schema;  // bind()-resolved; real output column order
};

struct VgiTableInOutCursor {
    sqlite3_vtab_cursor base;  // must be first member
    // caller persists across every xFilter call this cursor sees (one
    // bind+init+stream reused for the whole correlated scan, not
    // reopened per outer row) - see table_in_out_caller.h's file comment
    // on why that's both correct and the point of this design.
    std::unique_ptr<TableInOutCaller> caller;
    std::shared_ptr<arrow::RecordBatch> current_batch;   // this row's Exchange() response
    std::shared_ptr<arrow::RecordBatch> current_input_row;  // for reading a HIDDEN column back
    int64_t row_in_batch = 0;
    int64_t rowid = 0;
    bool eof = true;  // true until the first xFilter runs
};

int SetVtabError(sqlite3_vtab* vtab, const std::string& message) {
    if (vtab->zErrMsg) sqlite3_free(vtab->zErrMsg);
    vtab->zErrMsg = sqlite3_mprintf("%s", message.c_str());
    return SQLITE_ERROR;
}

int ConnectImpl(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** vtab_out,
                char** err) {
    auto* pool = reinterpret_cast<ConnectionPool*>(pAux);
    auto args = ParseModuleArgs(argc, argv);
    auto require = [&](const char* key) -> std::optional<std::string> {
        auto it = args.find(key);
        if (it == args.end() || it->second.empty()) return std::nullopt;
        return it->second;
    };
    auto location = require("location");
    auto catalog_name = require("catalog");
    auto schema_name = require("schema");
    auto function_name = require("function");
    if (!location || !catalog_name || !schema_name || !function_name) {
        *err = sqlite3_mprintf(
            "vgi_table_in_out requires location=, catalog=, schema=, and function= arguments");
        return SQLITE_ERROR;
    }

    auto vtab = std::make_unique<VgiTableInOutVtab>();
    std::memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->pool = pool;
    vtab->location = *location;
    vtab->catalog_name = *catalog_name;
    vtab->schema_name = *schema_name;
    vtab->function_name = *function_name;
    try {
        // Discover the function's declared input shape, then bind once
        // (a throwaway connection - released when `caller` goes out of
        // scope at the end of this function) purely to learn the real
        // output schema for sqlite3_declare_vtab. A real query's own
        // cursor does its own separate bind later (xFilter) - see the
        // struct's own field comment and table_in_out_caller.h's file
        // comment on why binds aren't shared/cached across connections.
        auto checkout = pool->Acquire(*location, *catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        auto fn = catalog.TableInOutFunctionGet(checkout->attach_opaque_data, *schema_name, *function_name);
        vtab->input_schema = fn.input_schema;

        TableInOutCaller caller(*pool, *location, *catalog_name, fn.schema_name, fn.function_name,
                                fn.input_schema);
        vtab->output_schema = caller.Bind();
    } catch (const std::exception& e) {
        *err = sqlite3_mprintf("vgi_table_in_out: %s", e.what());
        return SQLITE_ERROR;
    }

    // Real-world functions collide output/argument names constantly (e.g.
    // open-meteo's `geocoding(name, count, country_code, language)`, whose
    // output rows also carry `name`/`country_code` columns) - confirmed
    // against a live worker, not a contrived case: every one of that
    // worker's 13 blended functions hit this, so silently refusing to
    // register any of them (this module's original stance - see the
    // header's file comment history) turned out to be a real, practically-
    // blocking limitation, not a narrow edge case worth leaving
    // undisambiguated. A hidden column's NAME only matters for two things
    // - CREATE TABLE uniqueness, and an explicit `SELECT hidden_col FROM
    // fn(...)` read-back (see xColumn) - never for call-syntax argument
    // binding, which is purely positional (SQLite's own table-valued-
    // function convention, confirmed in this module's own design notes).
    // So a colliding hidden column can be safely renamed without changing
    // what any real query can do: append "_arg" (and "_arg2", "_arg3", ...
    // if that's *also* taken - pathological, but a deterministic fallback
    // beats an infinite loop or a silent failure) until it's unique against
    // every name declared so far.
    std::vector<std::string> declared_names;
    for (int i = 0; i < vtab->output_schema->num_fields(); ++i) {
        declared_names.push_back(vtab->output_schema->field(i)->name());
    }
    auto disambiguate = [&](const std::string& name) {
        if (std::find(declared_names.begin(), declared_names.end(), name) == declared_names.end()) return name;
        for (int suffix = 1;; ++suffix) {
            std::string candidate = name + "_arg" + (suffix == 1 ? "" : std::to_string(suffix));
            if (std::find(declared_names.begin(), declared_names.end(), candidate) == declared_names.end()) {
                return candidate;
            }
        }
    };

    std::ostringstream ddl;
    ddl << "CREATE TABLE x(";
    bool first = true;
    for (int i = 0; i < vtab->output_schema->num_fields(); ++i) {
        if (!first) ddl << ", ";
        first = false;
        const auto& field = vtab->output_schema->field(i);
        ddl << SqlQuoteIdentifier(field->name()) << " " << SqliteDeclaredType(field->type());
    }
    for (int i = 0; i < vtab->input_schema->num_fields(); ++i) {
        if (!first) ddl << ", ";
        first = false;
        const auto& field = vtab->input_schema->field(i);
        // HIDDEN: bound from the table-valued-function call site's
        // positional arguments (or an explicit WHERE = constraint), never
        // returned by a plain SELECT * - see the file comment on why this
        // is what makes correlated `FROM t, fn(t.x)` work with no extra
        // code.
        std::string hidden_name = disambiguate(field->name());
        declared_names.push_back(hidden_name);
        ddl << SqlQuoteIdentifier(hidden_name) << " " << SqliteDeclaredType(field->type()) << " HIDDEN";
    }
    ddl << ")";
    if (int rc = sqlite3_declare_vtab(db, ddl.str().c_str()); rc != SQLITE_OK) {
        *err = sqlite3_mprintf("vgi_table_in_out: sqlite3_declare_vtab failed: %s", ddl.str().c_str());
        return rc;
    }

    *vtab_out = reinterpret_cast<sqlite3_vtab*>(vtab.release());
    return SQLITE_OK;
}

int xCreate(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** vtab_out,
            char** err) {
    return ConnectImpl(db, pAux, argc, argv, vtab_out, err);
}

int xConnect(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** vtab_out,
             char** err) {
    return ConnectImpl(db, pAux, argc, argv, vtab_out, err);
}

int xDisconnect(sqlite3_vtab* vtab) {
    delete reinterpret_cast<VgiTableInOutVtab*>(vtab);
    return SQLITE_OK;
}

int xDestroy(sqlite3_vtab* vtab) { return xDisconnect(vtab); }

int xBestIndex(sqlite3_vtab* base_vtab, sqlite3_index_info* info) {
    auto* vtab = reinterpret_cast<VgiTableInOutVtab*>(base_vtab);
    const int num_output = vtab->output_schema->num_fields();
    const int num_hidden = vtab->input_schema->num_fields();

    // Every hidden column needs exactly one usable EQ constraint, claimed
    // in a fixed order (hidden column i -> argvIndex i+1) - see the file
    // comment on why no idxStr encoding is needed for that fixed mapping.
    for (int hidden_i = 0; hidden_i < num_hidden; ++hidden_i) {
        const int declared_col = num_output + hidden_i;
        bool found = false;
        for (int j = 0; j < info->nConstraint; ++j) {
            const auto& constraint = info->aConstraint[j];
            if (constraint.usable && constraint.iColumn == declared_col &&
                constraint.op == SQLITE_INDEX_CONSTRAINT_EQ) {
                info->aConstraintUsage[j].argvIndex = hidden_i + 1;
                info->aConstraintUsage[j].omit = 1;  // see the file comment
                found = true;
                break;
            }
        }
        if (!found) {
            // This plan can't bind every required argument - reject it
            // outright (see the file comment's xBestIndex section).
            return SQLITE_CONSTRAINT;
        }
    }

    // A per-row RPC round trip is real work, unlike a plain in-memory
    // vtab - a flat, modest-but-not-trivial placeholder (this driver has
    // no real per-call cost signal to offer SQLite's planner yet, same
    // documented-gap shape as vgi_worker's own pre-cardinality-estimate
    // placeholder from Milestone 3, just permanently so here since there
    // is no worker-declared cardinality for a function call at all).
    info->estimatedCost = 100.0;
    info->estimatedRows = 1;
    return SQLITE_OK;
}

int xOpen(sqlite3_vtab*, sqlite3_vtab_cursor** cursor_out) {
    auto cursor = std::make_unique<VgiTableInOutCursor>();
    std::memset(&cursor->base, 0, sizeof(cursor->base));
    *cursor_out = reinterpret_cast<sqlite3_vtab_cursor*>(cursor.release());
    return SQLITE_OK;
}

int xClose(sqlite3_vtab_cursor* cursor) {
    delete reinterpret_cast<VgiTableInOutCursor*>(cursor);
    return SQLITE_OK;
}

int xFilter(sqlite3_vtab_cursor* base_cursor, int, const char*, int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<VgiTableInOutCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiTableInOutVtab*>(base_cursor->pVtab);
    const int num_hidden = vtab->input_schema->num_fields();
    if (argc != num_hidden) {
        // Shouldn't happen given xBestIndex always claims exactly
        // num_hidden constraints - defensive, not a normal-path check.
        return SetVtabError(base_cursor->pVtab, "vgi_table_in_out: internal error - argument count mismatch");
    }
    try {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (int i = 0; i < num_hidden; ++i) {
            auto scalar = BuildArrowScalarFromSqliteValue(argv[i], vtab->input_schema->field(i)->type());
            if (!scalar) {
                return SetVtabError(base_cursor->pVtab, "vgi_table_in_out: argument \"" +
                                                             vtab->input_schema->field(i)->name() +
                                                             "\" doesn't match its declared type");
            }
            auto array_result = arrow::MakeArrayFromScalar(*scalar, 1);
            if (!array_result.ok()) {
                return SetVtabError(base_cursor->pVtab, "vgi_table_in_out: building argument \"" +
                                                             vtab->input_schema->field(i)->name() +
                                                             "\": " + array_result.status().ToString());
            }
            columns.push_back(array_result.ValueUnsafe());
        }
        auto input_row = arrow::RecordBatch::Make(vtab->input_schema, 1, columns);

        if (!cursor->caller) {
            // Lazily bound on this cursor's first xFilter, then reused
            // for every later xFilter call on the SAME cursor (one
            // caller per correlated scan, not per row) - see
            // table_in_out_caller.h's file comment.
            cursor->caller = std::make_unique<TableInOutCaller>(*vtab->pool, vtab->location, vtab->catalog_name,
                                                                 vtab->schema_name, vtab->function_name,
                                                                 vtab->input_schema);
        }
        cursor->current_input_row = input_row;
        cursor->current_batch = cursor->caller->Exchange(input_row);
        cursor->row_in_batch = 0;
        cursor->eof = !cursor->current_batch || cursor->current_batch->num_rows() == 0;
    } catch (const std::exception& e) {
        return SetVtabError(base_cursor->pVtab, e.what());
    }
    return SQLITE_OK;
}

int xNext(sqlite3_vtab_cursor* base_cursor) {
    auto* cursor = reinterpret_cast<VgiTableInOutCursor*>(base_cursor);
    ++cursor->rowid;
    ++cursor->row_in_batch;
    if (!cursor->current_batch || cursor->row_in_batch >= cursor->current_batch->num_rows()) {
        cursor->eof = true;
    }
    return SQLITE_OK;
}

int xEof(sqlite3_vtab_cursor* base_cursor) {
    return reinterpret_cast<VgiTableInOutCursor*>(base_cursor)->eof ? 1 : 0;
}

int xColumn(sqlite3_vtab_cursor* base_cursor, sqlite3_context* ctx, int col_idx) {
    auto* cursor = reinterpret_cast<VgiTableInOutCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiTableInOutVtab*>(base_cursor->pVtab);
    const int num_output = vtab->output_schema->num_fields();
    if (col_idx < num_output) {
        if (!cursor->current_batch || col_idx >= cursor->current_batch->num_columns()) {
            sqlite3_result_null(ctx);
            return SQLITE_OK;
        }
        SetSqliteResultFromArrow(ctx, *cursor->current_batch->column(col_idx), cursor->row_in_batch);
        return SQLITE_OK;
    }
    // A HIDDEN argument column, explicitly selected back (unusual, but
    // real - SQLite allows it, and json_each's own hidden columns support
    // it too). current_input_row is always exactly 1 row (this call's
    // bound argument values) - see xFilter.
    const int hidden_i = col_idx - num_output;
    if (!cursor->current_input_row || hidden_i >= cursor->current_input_row->num_columns()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    SetSqliteResultFromArrow(ctx, *cursor->current_input_row->column(hidden_i), 0);
    return SQLITE_OK;
}

int xRowid(sqlite3_vtab_cursor* base_cursor, sqlite3_int64* rowid_out) {
    *rowid_out = reinterpret_cast<VgiTableInOutCursor*>(base_cursor)->rowid;
    return SQLITE_OK;
}

const sqlite3_module kVgiTableInOutModule = {
    /* iVersion */ 0,
    /* xCreate */ xCreate,
    /* xConnect */ xConnect,
    /* xBestIndex */ xBestIndex,
    /* xDisconnect */ xDisconnect,
    /* xDestroy */ xDestroy,
    /* xOpen */ xOpen,
    /* xClose */ xClose,
    /* xFilter */ xFilter,
    /* xNext */ xNext,
    /* xEof */ xEof,
    /* xColumn */ xColumn,
    /* xRowid */ xRowid,
    /* xUpdate */ nullptr,  // read-only - see the file comment
    /* xBegin */ nullptr,   // no transaction coordination - see the file comment
    /* xSync */ nullptr,
    /* xCommit */ nullptr,
    /* xRollback */ nullptr,
    /* xFindFunction */ nullptr,
    /* xRename */ nullptr,
    /* xSavepoint */ nullptr,
    /* xRelease */ nullptr,
    /* xRollbackTo */ nullptr,
    /* xShadowName */ nullptr,
    /* xIntegrity */ nullptr,
};

}  // namespace

int RegisterVgiTableInOutModule(sqlite3* db, ConnectionPool* pool) {
    // No destructor callback here (unlike RegisterVgiWorkerModule): this
    // module doesn't own `pool` - it shares the one the vgi_worker module
    // (or, if that module is ever registered after this one, whichever
    // registers first) already owns and will destroy.
    return sqlite3_create_module_v2(db, "vgi_table_in_out", &kVgiTableInOutModule, pool, nullptr);
}

}  // namespace vgi_sqlite
