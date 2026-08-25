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
// exactly that reason), so it carries both pushdown decisions: the
// projected column list, and the pushed-down WHERE constraints (in
// argvIndex order, so position i here lines up with xFilter's argv[i]) -
// format "P<cols>|C<col>:<op>,...", either half empty when there's
// nothing to push for it.
std::string EncodeIdxStr(const std::vector<int>& projected_columns,
                          const std::vector<PushableConstraint>& constraints) {
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
    return out;
}

struct DecodedIdxStr {
    std::vector<int> projected_columns;
    std::vector<PushableConstraint> constraints;
};

DecodedIdxStr DecodeIdxStr(const char* idxStr) {
    DecodedIdxStr decoded;
    if (!idxStr) return decoded;
    std::string s(idxStr);
    auto bar = s.find('|');
    std::string proj = (bar == std::string::npos) ? "" : s.substr(1, bar - 1);
    std::string cons = (bar == std::string::npos) ? "" : s.substr(bar + 2);  // skip "|C"

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
            ddl << "\"" << field->name() << "\" " << SqliteDeclaredType(field->type());
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
    // No ORDER BY/LIMIT pushdown yet - SQLite still sorts/limits itself,
    // always correct just not as cheap as it could be (later Milestone-3
    // follow-up).
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

    // WHERE-constraint pushdown (see vtab/filter_pushdown.h) - `constraints`
    // (computed above, alongside the cost estimate) claims argvIndex for
    // each pushable constraint but never sets .omit, so SQLite always
    // re-checks correctness itself regardless of whether the worker
    // actually applies the filter.
    auto encoded = EncodeIdxStr(projected, constraints);
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
        pushdown_filters = EncodePushdownFilters(vtab->table.columns, decoded.constraints, argv);
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
        // BindRequest.schema_name exists to disambiguate a function name
        // registered in more than one schema; leaving it null (as here)
        // is exactly what asks the worker to fall back to a cross-schema
        // lookup by name instead, which every fixture table this driver
        // has scanned so far resolves correctly through. Revisit only if
        // a real name collision across schemas turns up.
        cursor->scanner->Bind(*vtab->table.scan_function);
        std::vector<int64_t> projection_ids(cursor->projected_columns.begin(),
                                             cursor->projected_columns.end());
        cursor->scanner->Init(projection_ids, pushdown_filters);
    } catch (const std::exception& e) {
        return SetVtabError(base_cursor->pVtab, e.what());
    }
    cursor->rowid = 0;
    AdvanceBatch(cursor);
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

int xColumn(sqlite3_vtab_cursor* base_cursor, sqlite3_context* ctx, int col_idx) {
    auto* cursor = reinterpret_cast<VgiCursor*>(base_cursor);
    if (!cursor->current_batch) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    // When a projection was requested AND the worker actually honored it,
    // the returned batch has only the requested columns, in request order
    // - translate the declared-table column index into its position
    // there. Checked against the batch's own width, not just "did we ask
    // for one": a function that doesn't declare projection_pushdown
    // support silently ignores InitRequest.projection_ids and returns
    // every column anyway (observed against vgi-fixture-worker's
    // cache_multicol - not documented anywhere read ahead of time, and
    // trusting the request without checking read the wrong column
    // outright). Width-matching the actual response is the only way to
    // tell honored-projection from ignored-projection from here.
    int position = col_idx;
    if (!cursor->projected_columns.empty() &&
        cursor->current_batch->num_columns() == static_cast<int>(cursor->projected_columns.size())) {
        auto it =
            std::find(cursor->projected_columns.begin(), cursor->projected_columns.end(), col_idx);
        if (it == cursor->projected_columns.end()) {
            // SQLite asked for a column it told xBestIndex it didn't need -
            // shouldn't happen, but null beats reading the wrong column.
            sqlite3_result_null(ctx);
            return SQLITE_OK;
        }
        position = static_cast<int>(it - cursor->projected_columns.begin());
    }
    if (position < 0 || position >= cursor->current_batch->num_columns()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    SetSqliteResultFromArrow(ctx, *cursor->current_batch->column(position), cursor->row_in_batch);
    return SQLITE_OK;
}

int xRowid(sqlite3_vtab_cursor* base_cursor, sqlite3_int64* rowid_out) {
    *rowid_out = reinterpret_cast<VgiCursor*>(base_cursor)->rowid;
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
    /* xUpdate */ nullptr,  // read-only; Milestone 4 (writable tables)
    /* xBegin */ nullptr,
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
