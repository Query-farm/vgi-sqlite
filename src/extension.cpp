// © Copyright 2026 Query Farm LLC - https://query.farm
//
// The loadable extension entrypoint: registers the vgi_worker virtual
// table module and the vgi_attach() discovery function.
//
// SQLITE_EXTENSION_INIT1 here (paired with every other sqlite3-calling
// file in this module - vgi_vtab.cpp, type_mapping.cpp - doing INIT3):
// resolves every sqlite3_* call through the *host's* function-pointer
// table (pApi) rather than linking a second, independently-initialized
// copy of SQLite. Tried the simpler-looking alternative (link the real
// libsqlite3.a straight into this .so, plain <sqlite3.h> everywhere) first
// - it crashed immediately on .load, even before calling anything: SQLite
// carries real process-global mutable state (default allocator, PRNG,
// VFS registry, mutex objects), and two independently-initialized copies
// in one process collide despite being the identical amalgamation
// version. Not a portability nicety - it's required for correctness.
#include <string>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "catalog/catalog_client.h"
#include "vtab/connection_pool.h"
#include "vtab/vgi_vtab.h"

namespace vgi_sqlite {
namespace {

// Single-quotes a value for embedding in a CREATE VIRTUAL TABLE argument,
// per SQL string-literal escaping (double any embedded quote).
std::string SqlQuote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "''";
        else out += c;
    }
    out += "'";
    return out;
}

// vgi_attach(location, catalog) -> integer count of tables discovered and
// declared as virtual tables. Uses the same ConnectionPool the vgi_worker
// module itself uses (sqlite3_user_data), so this discovery call and
// every table it creates share one spawned worker process per
// (location, catalog) rather than each opening its own. Issues one CREATE
// VIRTUAL TABLE per table found; re-running against the same catalog
// refreshes (DROP + CREATE each table).
void VgiAttachFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2) {
        sqlite3_result_error(ctx, "vgi_attach(location, catalog) takes exactly 2 arguments", -1);
        return;
    }
    auto location = std::string(reinterpret_cast<const char*>(sqlite3_value_text(argv[0])));
    auto catalog_name = std::string(reinterpret_cast<const char*>(sqlite3_value_text(argv[1])));
    sqlite3* db = sqlite3_context_db_handle(ctx);
    auto* pool = reinterpret_cast<ConnectionPool*>(sqlite3_user_data(ctx));

    int table_count = 0;
    int skip_count = 0;
    try {
        auto pooled = pool->GetOrCreate(location, catalog_name);
        VgiCatalogClient catalog(pooled->connection);

        for (const auto& schema : catalog.Schemas(pooled->attach_opaque_data)) {
            for (const auto& table : catalog.SchemaContentsTables(pooled->attach_opaque_data, schema.name)) {
                std::string vtab_name = schema.name + "." + table.name;
                std::string drop_sql = "DROP TABLE IF EXISTS \"" + vtab_name + "\";";
                std::string create_sql = "CREATE VIRTUAL TABLE \"" + vtab_name + "\" USING vgi_worker(" +
                                          "location=" + SqlQuote(location) +
                                          ", catalog=" + SqlQuote(catalog_name) +
                                          ", schema=" + SqlQuote(schema.name) +
                                          ", table=" + SqlQuote(table.name) + ");";
                char* err = nullptr;
                if (sqlite3_exec(db, drop_sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
                    // A DROP failing (rather than the CREATE that follows)
                    // means something is structurally wrong with this
                    // connection, not this one table - not worth
                    // continuing past.
                    std::string msg = "vgi_attach: " + (err ? std::string(err) : "drop failed");
                    if (err) sqlite3_free(err);
                    sqlite3_result_error(ctx, msg.c_str(), -1);
                    return;
                }
                // A single table's xConnect can fail for reasons specific
                // to that table (a worker that hasn't implemented
                // catalog_table_scan_function_get for a non-declarative
                // table, observed against vgi-python's own fixture
                // worker) without the rest of the catalog being any less
                // usable - skip and keep going rather than abort the
                // whole attach on one bad table.
                if (sqlite3_exec(db, create_sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
                    sqlite3_log(SQLITE_WARNING, "vgi_attach: skipping \"%s\": %s", vtab_name.c_str(),
                                err ? err : "create failed");
                    if (err) sqlite3_free(err);
                    ++skip_count;
                    continue;
                }
                ++table_count;
            }
        }
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
        return;
    }
    if (skip_count > 0) {
        sqlite3_log(SQLITE_WARNING, "vgi_attach: %d table(s) skipped (see prior warnings)", skip_count);
    }
    sqlite3_result_int(ctx, table_count);
}

}  // namespace
}  // namespace vgi_sqlite

extern "C" int sqlite3_vgi_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    (void)pzErrMsg;

    // The module owns the pool (destroyed with it, see
    // RegisterVgiWorkerModule); vgi_attach() just borrows the same
    // pointer via sqlite3_create_function's client-data argument so its
    // discovery connection is shared with every table it creates.
    auto* pool = vgi_sqlite::RegisterVgiWorkerModule(db);
    if (!pool) return SQLITE_ERROR;

    return sqlite3_create_function(db, "vgi_attach", 2, SQLITE_UTF8, pool, vgi_sqlite::VgiAttachFunc,
                                    nullptr, nullptr);
}
