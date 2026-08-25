// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/vgi_vtab.h"

#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>

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

// One vgi_worker table, backed by a pooled (shared with every other table
// from the same location+catalog) connection.
struct VgiVtab {
    sqlite3_vtab base;  // must be first member (sqlite3 vtab ABI)
    std::shared_ptr<PooledConnection> pooled;
    CatalogTable table;
};

struct VgiCursor {
    sqlite3_vtab_cursor base;  // must be first member
    std::unique_ptr<TableScanner> scanner;
    std::shared_ptr<arrow::RecordBatch> current_batch;
    int64_t row_in_batch = 0;
    int64_t rowid = 0;
    bool eof = false;
};

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
    try {
        vtab->pooled = pool->GetOrCreate(*location, *catalog_name);
        VgiCatalogClient catalog(vtab->pooled->connection);
        vtab->table = catalog.TableGet(vtab->pooled->attach_opaque_data, *schema_name, *table_name);
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

int xBestIndex(sqlite3_vtab*, sqlite3_index_info* info) {
    // MVP: no constraint/order pushdown yet - always a full scan. Cost is
    // deliberately high-ish (SQLite compares plans across joined tables)
    // so a table with a real cardinality estimate can out-rank this once
    // that's wired through (see the plan's Phase 3).
    info->estimatedCost = 1'000'000.0;
    info->estimatedRows = 1'000'000;
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

int xFilter(sqlite3_vtab_cursor* base_cursor, int, const char*, int, sqlite3_value**) {
    auto* cursor = reinterpret_cast<VgiCursor*>(base_cursor);
    auto* vtab = reinterpret_cast<VgiVtab*>(base_cursor->pVtab);
    try {
        cursor->scanner =
            std::make_unique<TableScanner>(vtab->pooled->connection, vtab->pooled->attach_opaque_data);
        if (!vtab->table.scan_function) {
            return SetVtabError(base_cursor->pVtab,
                                 "table has no scan_function (fallback RPC not wired into the vtab yet)");
        }
        cursor->scanner->Bind(*vtab->table.scan_function, vtab->table.schema_name);
        cursor->scanner->Init();
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
    if (!cursor->current_batch || col_idx < 0 || col_idx >= cursor->current_batch->num_columns()) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    SetSqliteResultFromArrow(ctx, *cursor->current_batch->column(col_idx), cursor->row_in_batch);
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
