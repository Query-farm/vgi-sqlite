// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/vgi_vtab.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <arrow/api.h>

// sqlite3ext.h + INIT3, not plain sqlite3.h: this file is only ever
// compiled into the vgi_extension loadable module, which must resolve
// every sqlite3_* call through the host's own function-pointer table
// rather than linking a second, independently-initialized copy of
// SQLite's process-global state (mutexes, allocator, VFS registry) -
// loading two real copies into one process crashes. See extension.cpp's
// file comment, which INIT1s the table this file's INIT3 references.
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3

#include "catalog/catalog_client.h"
#include "catalog/table_scanner.h"
#include "catalog/table_writer.h"
#include "sql_quote.h"
#include "types/type_mapping.h"
#include "vtab/connection_pool.h"
#include "vtab/filter_pushdown.h"

namespace vgi_sqlite {
namespace {

// Parses `key='value', key2='value2'` module-argument syntax. SQLite
// itself only splits the parenthesized USING(...) clause into per-token
// strings at commas outside of quotes/parens - the key=value structure
// within each token is this module's own convention to parse.
std::map<std::string, std::string> ParseModuleArgs(int argc, const char* const* argv) {
    // argv[0] = module name, argv[1] = db name ("main" etc.), argv[2] =
    // table name; the actual USING(...) arguments start at argv[3].
    std::map<std::string, std::string> args;
    for (int i = 3; i < argc; ++i) {
        std::string token(argv[i]);
        auto eq = token.find('=');
        if (eq == std::string::npos) continue;
        std::string key = token.substr(0, eq);
        std::string value = token.substr(eq + 1);
        // Trim surrounding whitespace, then a single layer of matching
        // quotes if present - SQLite hands us the raw token text.
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

// True for a declared column carrying VGI's is_row_id metadata (the
// worker-chosen per-row identifier column UPDATE/DELETE key writes by -
// see table_writer.h's file comment and TableWriter::Write).
bool IsRowIdColumn(const std::shared_ptr<arrow::Field>& field) {
    return field->metadata() && field->metadata()->Contains("is_row_id");
}

// The declared-column index of the field VGI marked as this table's row
// identifier, or nullopt if the table has none (no update/delete support,
// or a scan-only table that never declared one).
std::optional<int> FindRowIdColumn(const std::shared_ptr<arrow::Schema>& columns) {
    if (!columns) return std::nullopt;
    for (int i = 0; i < columns->num_fields(); ++i) {
        if (IsRowIdColumn(columns->field(i))) return i;
    }
    return std::nullopt;
}

// One vgi_worker table. Doesn't hold a connection long-term - only
// (location, catalog, schema, table) plus the pool to Acquire() a fresh
// checkout from whenever it actually needs one (xConnect's schema
// resolution; each xFilter's whole scan) - see connection_pool.h's file
// comment on why holding one connection per table for the table's entire
// lifetime would defeat the pool (a catalog with 50 tables would spawn 50
// worker processes just from attaching, exactly what ConnectionPool
// exists to avoid).
struct VgiVtab {
    sqlite3_vtab base;  // must be first member (sqlite3 vtab ABI)
    ConnectionPool* pool = nullptr;
    std::string location;
    std::string catalog_name;
    std::string schema_name;
    std::string table_name;
    CatalogTable table;
};

struct VgiCursor {
    sqlite3_vtab_cursor base;  // must be first member
    // Declared before `scanner` so it's destroyed *after* scanner (members
    // destroy in reverse declaration order): scanner's destructor must run
    // (closing its stream) before this checkout releases the connection
    // back to the pool for reuse, or a still-open stream would be handed
    // to the next caller.
    std::optional<ConnectionPool::Checkout> checkout;
    std::unique_ptr<TableScanner> scanner;
    std::shared_ptr<arrow::RecordBatch> current_batch;
    int64_t row_in_batch = 0;
    int64_t rowid = 0;
    bool eof = false;
    // The declared-table column indices actually fetched over the wire,
    // in fetch order, when xBestIndex found a real (not "select *")
    // column subset - empty means "every column", the common case. Set
    // from xFilter's idxStr; xColumn needs it to translate a declared
    // column index into its position within the (narrower) fetched batch.
    std::vector<int> projected_columns;
};

// idxStr is xBestIndex's only channel to xFilter (SQLite frees it via
// sqlite3_free per needToFreeIdxStr - built with sqlite3_mprintf for
// exactly that reason), so it carries every pushdown decision: the
// projected column list, the pushed-down WHERE constraints (in argvIndex
// order, so position i here lines up with xFilter's argv[i]), and -
// separately - whether a LIMIT constraint was also claimed and at which
// argvIndex - format "P<cols>|C<col>:<op>,...|L<argvIndex>", the last
// section empty when LIMIT wasn't pushed (see xBestIndex's comment on
// when that's safe).
std::string EncodeIdxStr(const std::vector<int>& projected_columns,
                          const std::vector<PushableConstraint>& constraints, int limit_argv_index) {
    std::string out = "P";
    for (size_t i = 0; i < projected_columns.size(); ++i) {
        if (i) out += ',';
        out += std::to_string(projected_columns[i]);
    }
    out += "|C";
    for (size_t i = 0; i < constraints.size(); ++i) {
        if (i) out += ',';
        out += std::to_string(constraints[i].column_index) + ':' + std::to_string(constraints[i].op);
    }
    out += "|L";
    if (limit_argv_index > 0) out += std::to_string(limit_argv_index);
    return out;
}

struct DecodedIdxStr {
    std::vector<int> projected_columns;
    std::vector<PushableConstraint> constraints;
    int limit_argv_index = 0;  // 0 = LIMIT not pushed; else 1-based argv[] position
};

DecodedIdxStr DecodeIdxStr(const char* idxStr) {
    DecodedIdxStr decoded;
    if (!idxStr) return decoded;
    std::string s(idxStr);
    auto bar1 = s.find('|');
    auto bar2 = (bar1 == std::string::npos) ? std::string::npos : s.find('|', bar1 + 1);
    std::string proj = (bar1 == std::string::npos) ? "" : s.substr(1, bar1 - 1);
    std::string cons = (bar1 == std::string::npos) ? ""
                        : (bar2 == std::string::npos) ? s.substr(bar1 + 2)  // skip "|C"
                                                       : s.substr(bar1 + 2, bar2 - bar1 - 2);
    std::string lim = (bar2 == std::string::npos) ? "" : s.substr(bar2 + 2);  // skip "|L"

    if (!proj.empty()) {
        std::istringstream iss(proj);
        std::string token;
        while (std::getline(iss, token, ',')) decoded.projected_columns.push_back(std::stoi(token));
    }
    if (!cons.empty()) {
        std::istringstream iss(cons);
        std::string token;
        while (std::getline(iss, token, ',')) {
            auto colon = token.find(':');
            if (colon == std::string::npos) continue;
            decoded.constraints.push_back(
                {std::stoi(token.substr(0, colon)),
                 static_cast<unsigned char>(std::stoi(token.substr(colon + 1)))});
        }
    }
    if (!lim.empty()) decoded.limit_argv_index = std::stoi(lim);
    return decoded;
}

int SetVtabError(sqlite3_vtab* vtab, const std::string& message) {
    if (vtab->zErrMsg) sqlite3_free(vtab->zErrMsg);
    vtab->zErrMsg = sqlite3_mprintf("%s", message.c_str());
    return SQLITE_ERROR;
}

// Shared by xCreate/xConnect: get or spawn the (location, catalog)'s
// shared connection, resolve the table, and declare its schema. Both
// callbacks do the same work - a vgi_worker table has no on-disk state to
// distinguish "first creation" from "load on reconnect".
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
    auto table_name = require("table");
    if (!location || !catalog_name || !schema_name || !table_name) {
        *err = sqlite3_mprintf(
            "vgi_worker requires location=, catalog=, schema=, and table= arguments");
        return SQLITE_ERROR;
    }

    auto vtab = std::make_unique<VgiVtab>();
    std::memset(&vtab->base, 0, sizeof(vtab->base));
    vtab->pool = pool;
    vtab->location = *location;
    vtab->catalog_name = *catalog_name;
    vtab->schema_name = *schema_name;
    vtab->table_name = *table_name;
    try {
        // Briefly checked out, then released back to the pool at the end
        // of this scope - schema resolution is a single quick round trip,
        // not worth pinning a whole connection to this table for.
        auto checkout = pool->Acquire(*location, *catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        vtab->table = catalog.TableGet(checkout->attach_opaque_data, *schema_name, *table_name);
    } catch (const std::exception& e) {
        *err = sqlite3_mprintf("vgi_worker: %s", e.what());
        return SQLITE_ERROR;
    }

    std::ostringstream ddl;
    ddl << "CREATE TABLE x(";
    if (vtab->table.columns) {
        for (int i = 0; i < vtab->table.columns->num_fields(); ++i) {
            if (i) ddl << ", ";
            const auto& field = vtab->table.columns->field(i);
            // field->name() is worker-supplied catalog metadata, not a
            // driver-controlled constant - must go through
            // SqlQuoteIdentifier (see sql_quote.h's file comment), not a
            // bare "\"...\"" - an unescaped embedded `"` would otherwise
            // let a worker's column name break out of this identifier and
            // inject arbitrary structure into the DDL sqlite3_declare_vtab
            // parses. Found during this driver's Milestone 5 security
            // review.
            ddl << SqlQuoteIdentifier(field->name()) << " " << SqliteDeclaredType(field->type());
        }
    }
    ddl << ")";
    if (int rc = sqlite3_declare_vtab(db, ddl.str().c_str()); rc != SQLITE_OK) {
        *err = sqlite3_mprintf("vgi_worker: sqlite3_declare_vtab failed: %s", ddl.str().c_str());
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
    delete reinterpret_cast<VgiVtab*>(vtab);
    return SQLITE_OK;
}

// vgi_worker owns no on-disk state beyond the CREATE VIRTUAL TABLE
// statement SQLite already persists (the worker location/catalog/table
// live entirely in the module arguments) - DROP is the same as
// disconnecting.
int xDestroy(sqlite3_vtab* vtab) { return xDisconnect(vtab); }

int xBestIndex(sqlite3_vtab* base_vtab, sqlite3_index_info* info) {
    // No ORDER BY pushdown yet - SQLite still sorts itself, always correct
    // just not as cheap as it could be (later Milestone-3 follow-up: VGI's
    // InitRequest.order_by_* hints are Top-N-shaped - column/direction/
    // null-order plus their own row limit - genuinely distinct work from
    // the plain row_limit pushdown just below, not a natural extension of
    // it).
    //
    // Cost/row estimate: prefer the worker's own cardinality_estimate (or,
    // failing that, cardinality_max as a conservative upper bound) over a
    // guess - this is what lets SQLite's join planner correctly prefer
    // scanning a 50-row table before a 10,000-row one, rather than
    // treating every vgi_worker table as equally (and arbitrarily)
    // expensive. Falls back to the same high-ish placeholder as before
    // (1,000,000) only when the worker declared neither, so a genuinely
    // unknown-size table still loses out to any table SQLite *does* have
    // real statistics for, joined or not.
    auto* vtab = reinterpret_cast<VgiVtab*>(base_vtab);  // used throughout this function
    int64_t row_estimate = vtab->table.cardinality_estimate >= 0 ? vtab->table.cardinality_estimate
                            : vtab->table.cardinality_max >= 0   ? vtab->table.cardinality_max
                                                                  : 1'000'000;
    // Reward plans that push work to the worker: each pushable constraint
    // (computed below) makes this specific index strategy less costly
    // relative to a full scan of the same table under a different
    // strategy, even without real per-predicate selectivity from the
    // worker - a fixed per-constraint discount is the same heuristic
    // SQLite's own query planner falls back to when it has no ANALYZE
    // statistics for a real table. Cost is never allowed below 1.0.
    //
    // Computed once here (not re-derived further down): SelectPushableConstraints
    // also assigns each usable constraint's argvIndex as a side effect of
    // being called, so it must run exactly once per xBestIndex invocation.
    auto constraints = SelectPushableConstraints(info);
    double discount = 1.0;
    for (size_t i = 0; i < constraints.size() && i < 4; ++i) discount *= 0.25;
    info->estimatedCost = std::max(1.0, static_cast<double>(row_estimate) * discount);
    info->estimatedRows = std::max<int64_t>(1, row_estimate);

    // Projection pushdown: colUsed's low 63 bits name individual declared
    // columns actually referenced by this query; bit 63 stands for "column
    // 63 and every higher-numbered column" too (sqlite3.h's own doc
    // comment on the field). A table with <=63 columns - true of every
    // fixture table seen so far - gets an exact subset; wider tables
    // degrade gracefully to "fetch everything" rather than fetching the
    // wrong columns, since colUsed can't distinguish "column 70" from
    // "column 90" once collapsed into bit 63.
    const int num_columns = vtab->table.columns ? vtab->table.columns->num_fields() : 0;
    std::vector<int> projected;
    if (num_columns > 0 && num_columns <= 63 && (info->colUsed & (1ULL << 63)) == 0) {
        for (int i = 0; i < num_columns; ++i) {
            if (info->colUsed & (1ULL << i)) projected.push_back(i);
        }
        // Every column referenced anyway (a real "SELECT *"): don't
        // bother encoding a subset that isn't one.
        if (static_cast<int>(projected.size()) == num_columns) projected.clear();
    }
    // A table this driver might UPDATE/DELETE needs its row-identifier
    // column's value on every fetched row regardless of what the query
    // actually selected - xRowid (see below) has to be able to report it
    // for every row this scan produces, since SQLite's own UPDATE/DELETE
    // plan against a vtab calls xUpdate with whatever xRowid reports for
    // each matched row. Force it into a real (non-empty) subset if it's
    // not already there; a "select everything" scan (empty `projected`)
    // already includes it with nothing to force.
    if (!projected.empty() && (vtab->table.supports_update || vtab->table.supports_delete)) {
        if (auto row_id_col = FindRowIdColumn(vtab->table.columns)) {
            if (std::find(projected.begin(), projected.end(), *row_id_col) == projected.end()) {
                projected.push_back(*row_id_col);
            }
        }
    }

    // WHERE-constraint pushdown (see vtab/filter_pushdown.h) - `constraints`
    // (computed above, alongside the cost estimate) claims argvIndex for
    // each pushable constraint but never sets .omit, so SQLite always
    // re-checks correctness itself regardless of whether the worker
    // actually applies the filter.
    //
    // LIMIT pushdown (InitRequest.row_limit, a plain "stop after this many
    // rows" hint - order_by_limit is the separate Top-N-hint field, not
    // this): only claimed when `constraints` is empty AND info->nOrderBy
    // is 0, i.e. this scan has no other pushed-down WHERE constraint and
    // no requested sort order. Necessary because this driver never sets
    // .omit on a WHERE constraint (see filter_pushdown.h's file comment) -
    // SQLite always re-verifies every row itself afterward, and if a
    // worker's early stop-after-N had already discarded rows that would
    // have failed that re-check but left true matches unseen further into
    // the table, the query could silently return fewer rows than the real
    // LIMIT. With zero unpushed-and-unverified WHERE work left for this
    // scan and no ordering requirement, "the worker's first N rows" and
    // "the query's first N rows" coincide, so it's safe. Like the WHERE
    // constraints above, .omit is still never set - SQLite applies its own
    // LIMIT to whatever this scan actually returns either way, so an
    // over-cautious worker that ignores the hint (or a worker that returns
    // more than N anyway) can't produce wrong results, only a missed
    // optimization.
    int limit_argv_index = 0;
    if (constraints.empty() && info->nOrderBy == 0) {
        for (int i = 0; i < info->nConstraint; ++i) {
            const auto& constraint = info->aConstraint[i];
            if (constraint.usable && constraint.op == SQLITE_INDEX_CONSTRAINT_LIMIT) {
                limit_argv_index = 1;
                info->aConstraintUsage[i].argvIndex = limit_argv_index;
                break;  // SQLite offers at most one LIMIT constraint per call
            }
        }
    }
    auto encoded = EncodeIdxStr(projected, constraints, limit_argv_index);
    info->idxStr = sqlite3_mprintf("%s", encoded.c_str());
    info->needToFreeIdxStr = 1;
    return SQLITE_OK;
}

int xOpen(sqlite3_vtab*, sqlite3_vtab_cursor** cursor_out) {
    auto cursor = std::make_unique<VgiCursor>();
    std::memset(&cursor->base, 0, sizeof(cursor->base));
    *cursor_out = reinterpret_cast<sqlite3_vtab_cursor*>(cursor.release());
    return SQLITE_OK;
}

int xClose(sqlite3_vtab_cursor* cursor) {
    delete reinterpret_cast<VgiCursor*>(cursor);
    return SQLITE_OK;
}

// Pull the next non-empty batch (or set eof) - shared by xFilter's first
// fetch and xNext's batch-boundary crossing.
void AdvanceBatch(VgiCursor* cursor) {
    auto next = cursor->scanner->Next();
    if (!next) {
        cursor->eof = true;
        cursor->current_batch = nullptr;
        return;
    }
    cursor->current_batch = *next;
    cursor->row_in_batch = 0;
}

int xFilter(sqlite3_vtab_cursor* base_cursor, int, const char* idxStr, int argc, sqlite3_value** argv) {
    auto* cursor = reinterpret_cast<VgiCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiVtab*>(base_cursor->pVtab);
    auto decoded = DecodeIdxStr(idxStr);
    cursor->projected_columns = decoded.projected_columns;
    // argv[i] holds the value for the constraint at argvIndex i+1 - the
    // same order SelectPushableConstraints assigned them in, so
    // decoded.constraints and argv line up positionally.
    std::optional<std::string> pushdown_filters;
    if (!decoded.constraints.empty() && static_cast<int>(decoded.constraints.size()) <= argc) {
        pushdown_filters =
            EncodePushdownFilters(vtab->table.columns, decoded.constraints, argv, decoded.projected_columns);
    }
    // Mutually exclusive with the constraints branch above (xBestIndex
    // only claims LIMIT when `constraints` was empty), so argv[0] is never
    // ambiguous between "the limit value" and "the first constraint's
    // value" - see xBestIndex's comment on why LIMIT pushdown is scoped
    // that way.
    std::optional<int64_t> row_limit;
    if (decoded.limit_argv_index > 0 && decoded.limit_argv_index <= argc) {
        row_limit = sqlite3_value_int64(argv[decoded.limit_argv_index - 1]);
    }
    try {
        // Checked out for this cursor's whole lifetime (released in
        // ~VgiCursor via `checkout`'s destructor, after `scanner`'s runs -
        // see the struct's field-order comment): a scan's producer stream
        // stays open across every xNext up to eof, so this connection is
        // genuinely busy for that whole span, not just for xFilter itself.
        cursor->checkout = vtab->pool->Acquire(vtab->location, vtab->catalog_name);
        cursor->scanner = std::make_unique<TableScanner>((*cursor->checkout)->connection,
                                                          (*cursor->checkout)->attach_opaque_data);
        if (!vtab->table.scan_function) {
            return SetVtabError(base_cursor->pVtab,
                                 "table has no scan_function (fallback RPC not wired into the vtab yet)");
        }
        // Not vtab->table.schema_name: a table's *backing function* isn't
        // necessarily registered under the same schema as the table
        // itself (observed against vgi-fixture-worker's
        // filter_echo_table - the table lives in "data", its
        // filter_echo_table_scan function is registered under "main").
        // Prefer the function's OWN schema (scan_function->schema_name,
        // protocol 1.5.0's ScanFunctionResult.schema_name - see
        // catalog_client.h's field comment) when the worker reported it;
        // only fall back to nullopt (asking the worker to cross-schema-
        // resolve by bare name) for a pre-1.5.0 worker that never sent it.
        // That fallback isn't a protocol guarantee every worker honors - a
        // real one (vgi-rust) refuses an unqualified bind outright - so
        // sending the real schema whenever it's known is the correct
        // behavior now, not just a defensive nicety.
        cursor->scanner->Bind(*vtab->table.scan_function, vtab->table.scan_function->schema_name,
                             vtab->pool->CurrentTransactionOpaqueData(vtab->location, vtab->catalog_name));
        std::vector<int64_t> projection_ids(cursor->projected_columns.begin(),
                                             cursor->projected_columns.end());
        cursor->scanner->Init(projection_ids, pushdown_filters, row_limit);
        cursor->rowid = 0;
        // Fetching the very first batch is just as capable of throwing (an
        // RPC "tick") as every later xNext-driven fetch below - must be
        // inside this same try/catch, not after it, or a worker error on
        // the first tick reaches std::terminate()/abort() instead of
        // surfacing as a normal SQLite error. See xNext's identical
        // try/catch around its own AdvanceBatch call.
        AdvanceBatch(cursor);
    } catch (const std::exception& e) {
        return SetVtabError(base_cursor->pVtab, e.what());
    }
    return SQLITE_OK;
}

int xNext(sqlite3_vtab_cursor* base_cursor) {
    auto* cursor = reinterpret_cast<VgiCursor*>(base_cursor);
    ++cursor->rowid;
    ++cursor->row_in_batch;
    if (!cursor->current_batch || cursor->row_in_batch >= cursor->current_batch->num_rows()) {
        try {
            AdvanceBatch(cursor);
        } catch (const std::exception& e) {
            return SetVtabError(base_cursor->pVtab, e.what());
        }
    }
    return SQLITE_OK;
}

int xEof(sqlite3_vtab_cursor* base_cursor) {
    return reinterpret_cast<VgiCursor*>(base_cursor)->eof ? 1 : 0;
}

// Translates a declared-table column index into its position within the
// current (possibly narrowed) fetched batch, or -1 if that column isn't
// present in it at all. Shared by xColumn and xRowid.
//
// When a projection was requested AND the worker actually honored it, the
// returned batch has only the requested columns, in request order - look
// the declared index up there. Checked against the batch's own width, not
// just "did we ask for one": a function that doesn't declare
// projection_pushdown support silently ignores InitRequest.projection_ids
// and returns every column anyway (observed against vgi-fixture-worker's
// cache_multicol - not documented anywhere read ahead of time, and
// trusting the request without checking read the wrong column outright).
// Width-matching the actual response is the only way to tell
// honored-projection from ignored-projection from here.
int FetchedPosition(VgiCursor* cursor, int declared_index) {
    if (!cursor->current_batch) return -1;
    if (!cursor->projected_columns.empty() &&
        cursor->current_batch->num_columns() == static_cast<int>(cursor->projected_columns.size())) {
        auto it = std::find(cursor->projected_columns.begin(), cursor->projected_columns.end(), declared_index);
        if (it == cursor->projected_columns.end()) return -1;
        return static_cast<int>(it - cursor->projected_columns.begin());
    }
    if (declared_index < 0 || declared_index >= cursor->current_batch->num_columns()) return -1;
    return declared_index;
}

int xColumn(sqlite3_vtab_cursor* base_cursor, sqlite3_context* ctx, int col_idx) {
    auto* cursor = reinterpret_cast<VgiCursor*>(base_cursor);
    int position = FetchedPosition(cursor, col_idx);
    if (position < 0) {
        // Either no current batch, or (shouldn't happen, but null beats
        // reading the wrong column) SQLite asked for a column it told
        // xBestIndex it didn't need.
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    SetSqliteResultFromArrow(ctx, *cursor->current_batch->column(position), cursor->row_in_batch);
    return SQLITE_OK;
}

// Reports VGI's own declared row-identifier column's value (see
// FindRowIdColumn) when the table has one and it's present in the current
// row's fetched batch - required for UPDATE/DELETE to name the right row
// (see xUpdate). Falls back to a purely local, scan-sequential counter for
// a table with no is_row_id column (or, defensively, one where its value
// couldn't be read this row) - fine for plain reads/`rowid` pseudo-column
// display, meaningless for identifying a row to a write function, which is
// exactly why xUpdate's UPDATE/DELETE paths require FindRowIdColumn to have
// found one before attempting either at all.
int xRowid(sqlite3_vtab_cursor* base_cursor, sqlite3_int64* rowid_out) {
    auto* cursor = reinterpret_cast<VgiCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiVtab*>(base_cursor->pVtab);
    if (auto row_id_col = FindRowIdColumn(vtab->table.columns)) {
        int position = FetchedPosition(cursor, *row_id_col);
        if (position >= 0) {
            auto arr = std::dynamic_pointer_cast<arrow::Int64Array>(cursor->current_batch->column(position));
            if (arr && !arr->IsNull(cursor->row_in_batch)) {
                *rowid_out = arr->Value(cursor->row_in_batch);
                return SQLITE_OK;
            }
        }
    }
    *rowid_out = cursor->rowid;
    return SQLITE_OK;
}

// Builds a 1-row batch from argv[2 + declared_index] for each field in
// `fields`/`declared_indices` (parallel arrays) - shared by INSERT's
// "every column but rowid" row and UPDATE's "every column but rowid, plus
// rowid appended last" row (see xUpdate).
arrow::Result<std::shared_ptr<arrow::RecordBatch>> BuildRowFromArgv(
    const arrow::FieldVector& fields, const std::vector<int>& declared_indices, sqlite3_value** argv, int argc) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (size_t i = 0; i < declared_indices.size(); ++i) {
        int argv_index = 2 + declared_indices[i];
        if (argv_index >= argc) {
            return arrow::Status::Invalid("missing a value for column \"" + fields[i]->name() + "\"");
        }
        auto scalar = BuildArrowScalarFromSqliteValue(argv[argv_index], fields[i]->type());
        if (!scalar) {
            return arrow::Status::Invalid("value for column \"" + fields[i]->name() +
                                           "\" doesn't match its declared type");
        }
        ARROW_ASSIGN_OR_RAISE(auto array, arrow::MakeArrayFromScalar(*scalar, 1));
        columns.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), 1, columns);
}

// INSERT: every user column except the is_row_id one (VGI's INSERT input
// schema, per table_writer.h's file comment) - argv[2 + declared_index]
// holds each column's new value, in declared order.
int DoInsert(VgiVtab* vtab, int argc, sqlite3_value** argv, sqlite3_int64* pRowid, sqlite3_vtab* base_vtab) {
    if (!vtab->table.supports_insert) {
        return SetVtabError(base_vtab, "vgi_worker: table doesn't support INSERT");
    }
    try {
        arrow::FieldVector fields;
        std::vector<int> declared_indices;
        const int num_columns = vtab->table.columns->num_fields();
        for (int i = 0; i < num_columns; ++i) {
            auto field = vtab->table.columns->field(i);
            if (IsRowIdColumn(field)) continue;
            fields.push_back(field);
            declared_indices.push_back(i);
        }
        auto row_result = BuildRowFromArgv(fields, declared_indices, argv, argc);
        if (!row_result.ok()) return SetVtabError(base_vtab, "vgi_worker: INSERT: " + row_result.status().ToString());

        auto checkout = vtab->pool->Acquire(vtab->location, vtab->catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        auto insert_fn =
            catalog.TableInsertFunctionGet(checkout->attach_opaque_data, vtab->schema_name, vtab->table_name);
        TableWriter writer(*vtab->pool, vtab->location, vtab->catalog_name);
        writer.Write(insert_fn, /*schema_name=*/std::nullopt, row_result.ValueUnsafe(),
                     vtab->pool->CurrentTransactionOpaqueData(vtab->location, vtab->catalog_name));

        // No real new-rowid to report: return_chunks=false means the
        // worker only echoes back a count, and even with a real
        // is_row_id column mapped for UPDATE/DELETE (see FindRowIdColumn),
        // a freshly-inserted row's assigned identity still isn't known
        // without RETURNING, which isn't requested (see table_writer.h).
        // Honor an explicit rowid if the INSERT gave one, else report 0 -
        // wrong for last_insert_rowid(), right for the INSERT itself. A
        // documented gap, not silently wrong.
        *pRowid = (sqlite3_value_type(argv[1]) != SQLITE_NULL) ? sqlite3_value_int64(argv[1]) : 0;
    } catch (const std::exception& e) {
        return SetVtabError(base_vtab, e.what());
    }
    return SQLITE_OK;
}

// UPDATE: every user column except is_row_id, with its (possibly
// unchanged - this driver doesn't try to detect which columns genuinely
// changed, see the file comment above DoUpdate's caller) new value, PLUS
// the is_row_id column appended last carrying the *old* rowid (argv[0]) -
// VGI's "(changed_columns..., rowid)" UPDATE input schema.
int DoUpdate(VgiVtab* vtab, int argc, sqlite3_value** argv, sqlite3_vtab* base_vtab) {
    if (!vtab->table.supports_update) {
        return SetVtabError(base_vtab, "vgi_worker: table doesn't support UPDATE");
    }
    auto row_id_col = FindRowIdColumn(vtab->table.columns);
    if (!row_id_col) {
        return SetVtabError(base_vtab,
                             "vgi_worker: table has no declared row-identifier column - can't UPDATE");
    }
    try {
        arrow::FieldVector fields;
        std::vector<int> declared_indices;
        const int num_columns = vtab->table.columns->num_fields();
        for (int i = 0; i < num_columns; ++i) {
            if (i == *row_id_col) continue;
            fields.push_back(vtab->table.columns->field(i));
            declared_indices.push_back(i);
        }
        auto row_result = BuildRowFromArgv(fields, declared_indices, argv, argc);
        if (!row_result.ok()) return SetVtabError(base_vtab, "vgi_worker: UPDATE: " + row_result.status().ToString());
        auto row = row_result.ValueUnsafe();

        // Append the rowid column (old identity, argv[0] - never the
        // proposed new one, argv[1]: this driver doesn't support changing
        // a row's own identity column via UPDATE, see the plan file).
        auto row_id_field = vtab->table.columns->field(*row_id_col);
        auto row_id_scalar = arrow::MakeScalar(static_cast<int64_t>(sqlite3_value_int64(argv[0])));
        auto row_id_arr_result = arrow::MakeArrayFromScalar(*row_id_scalar, 1);
        if (!row_id_arr_result.ok()) {
            return SetVtabError(base_vtab, "vgi_worker: UPDATE: building rowid column: " +
                                                row_id_arr_result.status().ToString());
        }
        auto with_rowid_result =
            row->AddColumn(row->num_columns(), row_id_field, row_id_arr_result.ValueUnsafe());
        if (!with_rowid_result.ok()) {
            return SetVtabError(base_vtab,
                                 "vgi_worker: UPDATE: appending rowid column: " + with_rowid_result.status().ToString());
        }

        auto checkout = vtab->pool->Acquire(vtab->location, vtab->catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        auto update_fn =
            catalog.TableUpdateFunctionGet(checkout->attach_opaque_data, vtab->schema_name, vtab->table_name);
        TableWriter writer(*vtab->pool, vtab->location, vtab->catalog_name);
        writer.Write(update_fn, /*schema_name=*/std::nullopt, with_rowid_result.ValueUnsafe(),
                     vtab->pool->CurrentTransactionOpaqueData(vtab->location, vtab->catalog_name));
    } catch (const std::exception& e) {
        return SetVtabError(base_vtab, e.what());
    }
    return SQLITE_OK;
}

// DELETE: just the is_row_id column (VGI's "(rowid,)" DELETE input
// schema), carrying argv[0] (the row to delete).
int DoDelete(VgiVtab* vtab, sqlite3_value** argv, sqlite3_vtab* base_vtab) {
    if (!vtab->table.supports_delete) {
        return SetVtabError(base_vtab, "vgi_worker: table doesn't support DELETE");
    }
    auto row_id_col = FindRowIdColumn(vtab->table.columns);
    if (!row_id_col) {
        return SetVtabError(base_vtab,
                             "vgi_worker: table has no declared row-identifier column - can't DELETE");
    }
    try {
        auto row_id_field = vtab->table.columns->field(*row_id_col);
        auto row_id_scalar = arrow::MakeScalar(static_cast<int64_t>(sqlite3_value_int64(argv[0])));
        auto row_id_arr_result = arrow::MakeArrayFromScalar(*row_id_scalar, 1);
        if (!row_id_arr_result.ok()) {
            return SetVtabError(base_vtab, "vgi_worker: DELETE: building rowid column: " +
                                                row_id_arr_result.status().ToString());
        }
        auto row = arrow::RecordBatch::Make(arrow::schema({row_id_field}), 1,
                                            std::vector<std::shared_ptr<arrow::Array>>{row_id_arr_result.ValueUnsafe()});

        auto checkout = vtab->pool->Acquire(vtab->location, vtab->catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        auto delete_fn =
            catalog.TableDeleteFunctionGet(checkout->attach_opaque_data, vtab->schema_name, vtab->table_name);
        TableWriter writer(*vtab->pool, vtab->location, vtab->catalog_name);
        writer.Write(delete_fn, /*schema_name=*/std::nullopt, row,
                     vtab->pool->CurrentTransactionOpaqueData(vtab->location, vtab->catalog_name));
    } catch (const std::exception& e) {
        return SetVtabError(base_vtab, e.what());
    }
    return SQLITE_OK;
}

// argc==1 is DELETE; argc>1 with a null argv[0] is INSERT; argc>1 with a
// non-null argv[0] is UPDATE (a changed rowid - argv[1] != argv[0] - isn't
// treated as a delete+reinsert the way SQLite's own docs allow a vtab to;
// this driver just ignores argv[1] and always updates the row named by
// argv[0], see DoUpdate's comment on why: VGI's own UPDATE model has no
// concept of changing the identity column's value in the first place).
int xUpdate(sqlite3_vtab* base_vtab, int argc, sqlite3_value** argv, sqlite3_int64* pRowid) {
    auto* vtab = reinterpret_cast<VgiVtab*>(base_vtab);
    if (!vtab->table.columns) {
        return SetVtabError(base_vtab, "vgi_worker: table has no known columns");
    }
    if (argc == 1) return DoDelete(vtab, argv, base_vtab);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) return DoInsert(vtab, argc, argv, pRowid, base_vtab);
    return DoUpdate(vtab, argc, argv, base_vtab);
}

// See connection_pool.h's file comment for the full transaction design:
// SQLite calls xBegin/xCommit/xRollback once per *vtab instance* per
// transaction, but VGI's catalog_transaction_begin/commit/rollback is
// scoped to one *(location, catalog) attachment* - ConnectionPool
// coordinates that mismatch (ref-counted per key), so these four
// callbacks are thin passthroughs. Not implemented: xSync (VGI has no
// two-phase pre-commit step to run - a plain SQLITE_OK no-op, with the
// real commit work happening in xCommit) and xSavepoint/xRelease/
// xRollbackTo (left null entirely - VGI has no savepoint-nesting RPCs at
// all, and SQLite doesn't require a vtab to implement them for ordinary
// single- or multi-statement transactions to work, only for explicit SQL
// SAVEPOINT nesting, which this driver doesn't support).
int xBegin(sqlite3_vtab* base_vtab) {
    auto* vtab = reinterpret_cast<VgiVtab*>(base_vtab);
    try {
        vtab->pool->BeginTransaction(vtab->location, vtab->catalog_name);
    } catch (const std::exception& e) {
        return SetVtabError(base_vtab, e.what());
    }
    return SQLITE_OK;
}

int xSync(sqlite3_vtab*) { return SQLITE_OK; }

int xCommit(sqlite3_vtab* base_vtab) {
    auto* vtab = reinterpret_cast<VgiVtab*>(base_vtab);
    try {
        vtab->pool->CommitTransaction(vtab->location, vtab->catalog_name);
    } catch (const std::exception& e) {
        return SetVtabError(base_vtab, e.what());
    }
    return SQLITE_OK;
}

int xRollback(sqlite3_vtab* base_vtab) {
    auto* vtab = reinterpret_cast<VgiVtab*>(base_vtab);
    try {
        vtab->pool->RollbackTransaction(vtab->location, vtab->catalog_name);
    } catch (const std::exception& e) {
        return SetVtabError(base_vtab, e.what());
    }
    return SQLITE_OK;
}

const sqlite3_module kVgiWorkerModule = {
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
    /* xUpdate */ xUpdate,  // INSERT/UPDATE/DELETE - see xUpdate's file comment
    /* xBegin */ xBegin,
    /* xSync */ xSync,
    /* xCommit */ xCommit,
    /* xRollback */ xRollback,
    /* xFindFunction */ nullptr,
    /* xRename */ nullptr,
    /* xSavepoint */ nullptr,
    /* xRelease */ nullptr,
    /* xRollbackTo */ nullptr,
    /* xShadowName */ nullptr,
    /* xIntegrity */ nullptr,
};

}  // namespace

ConnectionPool* RegisterVgiWorkerModule(sqlite3* db) {
    // sqlite3_create_module_v2's client-data destructor is how the pool
    // (one spawned worker process per location+catalog, shared by every
    // vgi_worker table that uses it) gets torn down when this db
    // connection closes or the module is replaced/unregistered - matching
    // the pool's lifetime to the module's rather than any one table's.
    auto* pool = new ConnectionPool();
    int rc = sqlite3_create_module_v2(db, "vgi_worker", &kVgiWorkerModule, pool,
                                       [](void* p) { delete reinterpret_cast<ConnectionPool*>(p); });
    if (rc != SQLITE_OK) return nullptr;  // pool already freed via the destructor above on failure
    return pool;
}

}  // namespace vgi_sqlite
