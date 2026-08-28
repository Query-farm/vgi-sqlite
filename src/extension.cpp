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
#include <map>
#include <set>
#include <string>
#include <vector>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <arrow/api.h>

#include "catalog/aggregate_caller.h"
#include "catalog/catalog_client.h"
#include "catalog/scalar_function_caller.h"
#include "sql_quote.h"
#include "types/type_mapping.h"
#include "vtab/connection_pool.h"
#include "vtab/vgi_table_function_vtab.h"
#include "vtab/vgi_table_in_out_vtab.h"
#include "vtab/vgi_vtab.h"

namespace vgi_sqlite {
namespace {

// Bridges a bound ScalarFunctionCaller (owned as this SQLite function's
// user-data, see registration below) into sqlite3_create_function_v2's
// callback shape: convert argv (natural-typed from each value's own
// SQLite storage class - see ScalarFunctionCaller's file comment on why
// there's no declared target type to convert against instead), call,
// convert the single-row result back.
void ScalarFunctionBridge(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* caller = reinterpret_cast<ScalarFunctionCaller*>(sqlite3_user_data(ctx));
    if (argc != caller->num_args()) {
        sqlite3_result_error(ctx, "argument count mismatch calling VGI scalar function", -1);
        return;
    }
    try {
        std::vector<std::shared_ptr<arrow::Scalar>> scalars;
        for (int i = 0; i < argc; ++i) {
            scalars.push_back(BuildArrowScalarFromSqliteValueNatural(argv[i]));
            if (!scalars.back()) {
                sqlite3_result_error(ctx, "VGI scalar functions don't accept a NULL argument here yet "
                                          "(its Arrow type can't be inferred from an absent value)",
                                      -1);
                return;
            }
        }
        auto result = caller->Call(scalars);
        SetSqliteResultFromArrow(ctx, *result->column(0), 0);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// Registration info for one VGI aggregate function - the SQLite function's
// user-data (shared across every SQLite aggregate context that function
// gets invoked with, unlike ScalarFunctionCaller which *is* the user-data
// directly). A fresh AggregateCaller is heap-allocated per SQLite
// aggregate context instead (see AggregateStepBridge) - one per GROUP BY
// group, or one for a whole-table aggregate - since each needs its own
// independent VGI execution_id (see aggregate_caller.h's file comment).
struct VgiAggregateFunctionEntry {
    ConnectionPool* pool;
    std::string location;
    std::string catalog_name;
    std::string function_name;
    int num_args;
    std::optional<std::string> schema_name;
};

// One row per SQL call, mirroring ScalarFunctionBridge: convert argv
// (natural-typed, same reasoning as scalar functions - FunctionInfo.arguments
// can't be trusted for real argument types), accumulate into this
// aggregate context's caller (lazily constructed on the first call for
// this context - sqlite3_aggregate_context zero-initializes new memory,
// so a null slot means "first row of this group").
void AggregateStepBridge(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    auto* entry = reinterpret_cast<VgiAggregateFunctionEntry*>(sqlite3_user_data(ctx));
    if (argc != entry->num_args) {
        sqlite3_result_error(ctx, "argument count mismatch calling VGI aggregate function", -1);
        return;
    }
    auto* slot = reinterpret_cast<AggregateCaller**>(sqlite3_aggregate_context(ctx, sizeof(AggregateCaller*)));
    if (!slot) {
        sqlite3_result_error_nomem(ctx);
        return;
    }
    try {
        if (!*slot) {
            *slot = new AggregateCaller(*entry->pool, entry->location, entry->catalog_name, entry->function_name,
                                        entry->num_args, entry->schema_name);
        }
        std::vector<std::shared_ptr<arrow::Scalar>> scalars;
        for (int i = 0; i < argc; ++i) {
            scalars.push_back(BuildArrowScalarFromSqliteValueNatural(argv[i]));
            if (!scalars.back()) {
                sqlite3_result_error(ctx, "VGI aggregate functions don't accept a NULL argument here yet "
                                          "(its Arrow type can't be inferred from an absent value)",
                                      -1);
                return;
            }
        }
        (*slot)->Step(scalars);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// Called exactly once per aggregate context, whether or not any row ever
// reached AggregateStepBridge (an aggregate over zero rows still gets one
// xFinal call, per SQLite's own contract) - sqlite3_aggregate_context(ctx, 0)
// (no allocation) returns null in that case, meaning no AggregateCaller
// was ever created; this driver reports NULL for an empty group rather
// than guessing a per-function identity value (0 for COUNT, NULL for SUM,
// ...) it has no way to know without ever binding.
void AggregateFinalBridge(sqlite3_context* ctx) {
    auto* slot = reinterpret_cast<AggregateCaller**>(sqlite3_aggregate_context(ctx, 0));
    if (!slot || !*slot) {
        sqlite3_result_null(ctx);
        return;
    }
    // Owns the caller from here regardless of outcome - guarantees its
    // destructor (best-effort aggregate_destructor RPC, see
    // aggregate_caller.h) runs exactly once, even on a Finalize() error.
    std::unique_ptr<AggregateCaller> caller(*slot);
    try {
        auto result = caller->Finalize();
        SetSqliteResultFromArrow(ctx, *result->column(0), 0);
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
    }
}

// vgi_attach(location, catalog[, bearer_token]) -> integer count of tables
// discovered and declared as virtual tables. Uses the same ConnectionPool
// the vgi_worker module itself uses (sqlite3_user_data), so this discovery
// call and every table it creates share one spawned worker process per
// (location, catalog) rather than each opening its own. Issues one CREATE
// VIRTUAL TABLE per table found; re-running against the same catalog
// refreshes (DROP + CREATE each table).
//
// bearer_token, when given (registered as a separate nArg=3 overload, not
// a NULL-able 3rd argument on one nArg=2 registration - see
// sqlite3_vgi_init below), only applies to an http:// or https:// location
// (VgiConnection::Connect's own HTTP-only auth mechanism - see its file
// comment) and is folded directly into `location`'s userinfo
// (`http://TOKEN@host:port/...`) before anything else touches it, so every
// downstream use of this same `location` string - this call's own
// checkout, and every CREATE VIRTUAL TABLE issued below - carries the
// token identically without a second, parallel channel to keep in sync.
void VgiAttachFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
    if (argc != 2 && argc != 3) {
        sqlite3_result_error(ctx, "vgi_attach(location, catalog[, bearer_token]) takes 2 or 3 arguments", -1);
        return;
    }
    auto location = std::string(reinterpret_cast<const char*>(sqlite3_value_text(argv[0])));
    auto catalog_name = std::string(reinterpret_cast<const char*>(sqlite3_value_text(argv[1])));
    if (argc == 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL) {
        auto bearer_token = std::string(reinterpret_cast<const char*>(sqlite3_value_text(argv[2])));
        if (!bearer_token.empty()) {
            bool is_http = location.rfind("http://", 0) == 0 || location.rfind("https://", 0) == 0;
            if (!is_http) {
                sqlite3_result_error(ctx, "vgi_attach: bearer_token is only valid for an http:// or https:// location",
                                      -1);
                return;
            }
            auto scheme_end = location.find("://") + 3;
            location = location.substr(0, scheme_end) + bearer_token + "@" + location.substr(scheme_end);
            // Every table this call creates persists this same `location`
            // (token included) verbatim in its CREATE VIRTUAL TABLE
            // statement - which SQLite itself stores, in cleartext, in
            // sqlite_master/sqlite_schema on disk (`SELECT sql FROM
            // sqlite_master` reads it back). That's not a bug introduced
            // here - vgi_attach() already persisted `location` this way
            // for the subprocess transport, where it's rarely secret - but
            // a bearer token is exactly the kind of value that shouldn't
            // be silently written to disk without the caller knowing.
            // Surfaced loudly (found during this driver's Milestone 5
            // security review) rather than fixed by a bigger redesign
            // (e.g. storing credentials outside the schema, as SQLite's
            // own ATTACH does for some encryption extensions) - out of
            // scope for now; see the plan file's Milestone 5 status.
            sqlite3_log(SQLITE_WARNING,
                        "vgi_attach: bearer_token will be stored in cleartext in this database's "
                        "sqlite_master (every vgi_worker table's CREATE VIRTUAL TABLE statement "
                        "embeds it) - only use vgi_attach()'s bearer_token argument against a "
                        "database file you're comfortable holding that credential");
        }
    }
    sqlite3* db = sqlite3_context_db_handle(ctx);
    auto* pool = reinterpret_cast<ConnectionPool*>(sqlite3_user_data(ctx));

    int table_count = 0;
    int skip_count = 0;
    try {
        // Checked out only for this discovery pass - released back to the
        // pool once every schema/table/function is enumerated, not held
        // for vgi_attach()'s caller's whole session (see connection_pool.h
        // and ScalarFunctionCaller's file comment: nothing here keeps a
        // connection long-term any more).
        auto checkout = pool->Acquire(location, catalog_name);
        VgiCatalogClient catalog(checkout->connection);

        // vgi_table_in_out and vgi_table_function both register into the
        // SAME flat "<catalog>_<function>" SQL table-name namespace (see
        // each loop's own comment) with no NATURAL per-arity dispatch -
        // a CREATE VIRTUAL TABLE name is just a name, SQL has no overload
        // resolution for it the way a function call has. Rather than
        // silently let a later overload's DROP+CREATE replace an earlier
        // one (worker-order-dependent, undetectable from here), both
        // loops below only COLLECT their candidates while iterating
        // schemas; the actual naming/registration happens in one unified
        // pass afterward (see "Register table_in_out + plain table
        // function candidates" below) that gives every genuine
        // arity-overload its own name instead of picking a winner.
        // Collected across the WHOLE attach call (every schema), matching
        // this flat namespace's own actual scope - a collision two
        // schemas apart is just as real as one within a single schema.
        // Ordinary tables (schema-qualified "schema.table" names) and
        // scalar/aggregate functions (SQLite's own (name, arg-count)-
        // keyed sqlite3_create_function_v2, which already disambiguates
        // by arity natively) don't need this - see CLAUDE.md's
        // "table_in_out and table_function" section.
        std::vector<CatalogTableInOutFunction> table_in_out_candidates;
        std::vector<CatalogPlainTableFunction> plain_table_fn_candidates;

        for (const auto& schema : catalog.Schemas(checkout->attach_opaque_data)) {
            for (const auto& table : catalog.SchemaContentsTables(checkout->attach_opaque_data, schema.name)) {
                std::string vtab_name = schema.name + "." + table.name;  // display/log use only - never embedded raw in SQL below
                std::string quoted_vtab_name = SqlQuoteIdentifier(vtab_name);
                std::string drop_sql = "DROP TABLE IF EXISTS " + quoted_vtab_name + ";";
                std::string create_sql = "CREATE VIRTUAL TABLE " + quoted_vtab_name + " USING vgi_worker(" +
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

            // Scalar functions: SQLite has one flat function namespace
            // (no catalog-qualified example.add_values(...) the way
            // DuckDB has), so every function is registered under
            // "<catalog>_<function>" - not exactly VGI's own opt-in
            // global_functions/global_function_prefix mechanism
            // (CatalogAttachResult), but the closest equivalent given
            // SQLite has no other addressing scheme to offer. Argument
            // types aren't checked here (unlike tables) - see
            // ScalarFunctionCaller's file comment on why they aren't
            // reliably known until the first real call; registration only
            // needs the argument *count*, which is known.
            for (const auto& fn : catalog.SchemaContentsScalarFunctions(checkout->attach_opaque_data, schema.name)) {
                if (!fn.argument_types) {
                    sqlite3_log(SQLITE_WARNING, "vgi_attach: skipping function \"%s.%s\": no argument count",
                                schema.name.c_str(), fn.name.c_str());
                    ++skip_count;
                    continue;
                }
                std::string sql_name = catalog_name + "_" + fn.name;
                // Not this discovery checkout: ScalarFunctionCaller
                // Acquire()s its own fresh connection for every call (see
                // its file comment) - it only needs the pool plus which
                // (location, catalog) to ask it for, not a connection
                // handed to it here.
                auto* caller = new ScalarFunctionCaller(*pool, location, catalog_name, fn.name,
                                                         fn.argument_types->num_fields(), fn.schema_name);
                int rc = sqlite3_create_function_v2(
                    db, sql_name.c_str(), fn.argument_types->num_fields(), SQLITE_UTF8, caller,
                    ScalarFunctionBridge, nullptr, nullptr,
                    [](void* p) { delete reinterpret_cast<ScalarFunctionCaller*>(p); });
                if (rc != SQLITE_OK) {
                    sqlite3_log(SQLITE_WARNING, "vgi_attach: failed to register function \"%s\": rc=%d",
                                sql_name.c_str(), rc);
                    ++skip_count;
                    continue;
                }
            }

            // Aggregate functions: same "<catalog>_<function>" flat
            // namespace as scalar functions, registered via
            // sqlite3_create_function_v2's xStep/xFinal rather than xFunc
            // - see AggregateStepBridge/AggregateFinalBridge. Windowed
            // aggregates (SQL OVER clauses) aren't registered here at all
            // (VGI's window RPC family is structurally separate and out
            // of scope - see aggregate_caller.h's file comment); every
            // aggregate this driver registers only ever answers a plain
            // GROUP BY or whole-table aggregation.
            for (const auto& fn : catalog.SchemaContentsAggregateFunctions(checkout->attach_opaque_data, schema.name)) {
                if (!fn.argument_types) {
                    sqlite3_log(SQLITE_WARNING, "vgi_attach: skipping aggregate \"%s.%s\": no argument count",
                                schema.name.c_str(), fn.name.c_str());
                    ++skip_count;
                    continue;
                }
                std::string sql_name = catalog_name + "_" + fn.name;
                auto* entry = new VgiAggregateFunctionEntry{pool, location, catalog_name, fn.name,
                                                            fn.argument_types->num_fields(), fn.schema_name};
                int rc = sqlite3_create_function_v2(
                    db, sql_name.c_str(), fn.argument_types->num_fields(), SQLITE_UTF8, entry,
                    /*xFunc=*/nullptr, AggregateStepBridge, AggregateFinalBridge,
                    [](void* p) { delete reinterpret_cast<VgiAggregateFunctionEntry*>(p); });
                if (rc != SQLITE_OK) {
                    sqlite3_log(SQLITE_WARNING, "vgi_attach: failed to register aggregate \"%s\": rc=%d",
                                sql_name.c_str(), rc);
                    ++skip_count;
                    continue;
                }
            }

            // table_in_out functions (the "blended"/RowTransformFunction
            // shape only - see catalog_client.h's CatalogTableInOutFunction
            // comment): each becomes its own vgi_table_in_out virtual
            // table, callable as a per-row-correlated table-valued
            // function (`SELECT * FROM t, "<catalog>_<function>"(t.x,
            // t.y)`, json_each(t.x)-style - see vtab/vgi_table_in_out_vtab.h).
            // Just collected here - see "Register table_in_out + plain
            // table function candidates" below for the actual naming/
            // registration, which needs every schema's candidates
            // together first.
            for (const auto& fn :
                 catalog.SchemaContentsTableInOutFunctions(checkout->attach_opaque_data, schema.name)) {
                table_in_out_candidates.push_back(fn);
            }

            // Plain, standalone table (generator) functions - e.g.
            // split_sequence - each become their own vgi_table_function
            // virtual table, callable both as a literal call
            // (`SELECT * FROM "<catalog>_<function>"(1, 2)`) and, per-outer-
            // row correlated, the same way vgi_table_in_out's functions are
            // (see vtab/vgi_table_function_vtab.h). Just collected here -
            // same reason as table_in_out functions above.
            for (const auto& fn :
                 catalog.SchemaContentsPlainTableFunctions(checkout->attach_opaque_data, schema.name)) {
                plain_table_fn_candidates.push_back(fn);
            }
        }

        // Register table_in_out + plain table function candidates: both
        // share one flat "<catalog>_<function>" SQL table-name namespace
        // (see the candidate-collection comment above), so naming has to
        // consider them together, across every schema, before issuing any
        // DDL. A base name used by exactly one candidate keeps that plain
        // name unchanged (the overwhelmingly common case - no behavior
        // change from before this arity-suffix scheme existed). A base
        // name shared by 2+ candidates is a genuine arity overload (e.g.
        // geo_encode/geo_encode3 both discovered as worker function name
        // "geo_encode" with different declared arities): each gets its
        // own name instead of one silently winning, by appending its own
        // argument count - "<catalog>_<function>_<N>args". This can't
        // collide with a worker's own separately-named overload (e.g. a
        // literal "geo_encode3") since that already has a different
        // function_name and was never grouped with "geo_encode" here in
        // the first place.
        struct FunctionVtabCandidate {
            std::string schema_name;
            std::string function_name;
            const char* module_name;  // "vgi_table_in_out" or "vgi_table_function"
            int arity;
        };
        std::vector<FunctionVtabCandidate> function_vtab_candidates;
        function_vtab_candidates.reserve(table_in_out_candidates.size() + plain_table_fn_candidates.size());
        for (const auto& fn : table_in_out_candidates) {
            // A null schema (distinct from a present-but-zero-field one -
            // see catalog_client.cpp's HasTableTypedArgument, which guards
            // the same way) means the worker's response had no usable
            // "arguments" field at all - treated as arity 0 rather than
            // dereferenced blindly.
            int arity = fn.input_schema ? fn.input_schema->num_fields() : 0;
            function_vtab_candidates.push_back({fn.schema_name, fn.function_name, "vgi_table_in_out", arity});
        }
        for (const auto& fn : plain_table_fn_candidates) {
            int arity = fn.argument_schema ? fn.argument_schema->num_fields() : 0;
            function_vtab_candidates.push_back({fn.schema_name, fn.function_name, "vgi_table_function", arity});
        }

        std::map<std::string, int> base_name_counts;
        for (const auto& c : function_vtab_candidates) {
            ++base_name_counts[catalog_name + "_" + c.function_name];
        }

        // Guards the rare residual case: two candidates sharing both a
        // base name AND an argument count (differing only by argument
        // TYPE, which a SQL identifier has no practical room to encode) -
        // arity alone can't disambiguate those, so the second one found
        // is skipped with a clear warning naming the collision, the same
        // deterministic-first-wins fallback this driver used for every
        // overload before this arity-suffix scheme existed.
        std::set<std::string> final_vtab_names;

        for (const auto& c : function_vtab_candidates) {
            std::string base_name = catalog_name + "_" + c.function_name;
            std::string vtab_name = base_name_counts[base_name] == 1
                                        ? base_name
                                        : base_name + "_" + std::to_string(c.arity) + "args";
            if (!final_vtab_names.insert(vtab_name).second) {
                sqlite3_log(SQLITE_WARNING,
                            "vgi_attach: skipping %s function \"%s.%s\" - \"%s\" is already registered "
                            "by another overload with the same argument count (arity alone can't "
                            "disambiguate overloads that also share their argument count; only the "
                            "first-discovered one of these is queryable)",
                            c.module_name, c.schema_name.c_str(), c.function_name.c_str(), vtab_name.c_str());
                ++skip_count;
                continue;
            }
            if (vtab_name != base_name) {
                sqlite3_log(SQLITE_NOTICE,
                            "vgi_attach: \"%s.%s\" (%s) registered as \"%s\", not \"%s\" - %d functions "
                            "named \"%s\" were discovered with different argument counts (an arity "
                            "overload), so each was given its own name instead of one silently winning",
                            c.schema_name.c_str(), c.function_name.c_str(), c.module_name, vtab_name.c_str(),
                            base_name.c_str(), base_name_counts[base_name], c.function_name.c_str());
            }
            std::string quoted_vtab_name = SqlQuoteIdentifier(vtab_name);
            std::string drop_sql = "DROP TABLE IF EXISTS " + quoted_vtab_name + ";";
            // `arity=` is always passed, not just when `vtab_name` got
            // suffixed - the suffix disambiguates the SQL TABLE name, but
            // xConnect/xCreate re-resolves the underlying function
            // independently (it only has this module's own arguments to
            // go on, not vgi_attach()'s own discovery-time context), so it
            // needs the SAME disambiguator threaded through explicitly or
            // it can't tell which overload THIS particular vtab instance
            // should bind to - see catalog_client.h's TableInOutFunctionGet/
            // PlainTableFunctionGet comment for the full story (a real bug
            // this driver's own regression test caught, not theoretical).
            std::string create_sql = "CREATE VIRTUAL TABLE " + quoted_vtab_name + " USING " +
                                      std::string(c.module_name) + "(" + "location=" + SqlQuote(location) +
                                      ", catalog=" + SqlQuote(catalog_name) + ", schema=" + SqlQuote(c.schema_name) +
                                      ", function=" + SqlQuote(c.function_name) +
                                      ", arity=" + SqlQuote(std::to_string(c.arity)) + ");";
            char* err = nullptr;
            if (sqlite3_exec(db, drop_sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
                std::string msg = "vgi_attach: " + (err ? std::string(err) : "drop failed");
                if (err) sqlite3_free(err);
                sqlite3_result_error(ctx, msg.c_str(), -1);
                return;
            }
            if (sqlite3_exec(db, create_sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
                sqlite3_log(SQLITE_WARNING, "vgi_attach: skipping %s function \"%s\": %s", c.module_name,
                            vtab_name.c_str(), err ? err : "create failed");
                if (err) sqlite3_free(err);
                ++skip_count;
                continue;
            }
            ++table_count;
        }
    } catch (const std::exception& e) {
        sqlite3_result_error(ctx, e.what(), -1);
        return;
    }
    if (skip_count > 0) {
        sqlite3_log(SQLITE_WARNING, "vgi_attach: %d table(s)/function(s) skipped (see prior warnings)", skip_count);
    }
    sqlite3_result_int(ctx, table_count);
}

}  // namespace
}  // namespace vgi_sqlite

// A loadable extension's init function must be visible from outside the
// DLL for SQLite's .load to find it via GetProcAddress - unlike ELF/
// Mach-O shared libraries, a Windows DLL exports nothing by default.
// __declspec(dllexport) is SQLite's own documented convention for this
// (no .def file needed for a single symbol).
#if defined(_WIN32)
extern "C" __declspec(dllexport) int sqlite3_vgi_init(sqlite3* db, char** pzErrMsg,
                                                        const sqlite3_api_routines* pApi) {
#else
extern "C" int sqlite3_vgi_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi) {
#endif
    SQLITE_EXTENSION_INIT2(pApi);
    (void)pzErrMsg;

    // The module owns the pool (destroyed with it, see
    // RegisterVgiWorkerModule); vgi_attach() just borrows the same
    // pointer via sqlite3_create_function's client-data argument so its
    // discovery connection is shared with every table it creates.
    auto* pool = vgi_sqlite::RegisterVgiWorkerModule(db);
    if (!pool) return SQLITE_ERROR;

    // Shares the same pool - see vgi_table_in_out_vtab.h's
    // RegisterVgiTableInOutModule doc comment on why this module doesn't
    // own/destroy it itself.
    if (int rc = vgi_sqlite::RegisterVgiTableInOutModule(db, pool); rc != SQLITE_OK) return rc;
    if (int rc = vgi_sqlite::RegisterVgiTableFunctionModule(db, pool); rc != SQLITE_OK) return rc;

    int rc = sqlite3_create_function(db, "vgi_attach", 2, SQLITE_UTF8, pool, vgi_sqlite::VgiAttachFunc,
                                      nullptr, nullptr);
    if (rc != SQLITE_OK) return rc;
    // Separate nArg=3 overload for the bearer_token form - see
    // VgiAttachFunc's file comment.
    return sqlite3_create_function(db, "vgi_attach", 3, SQLITE_UTF8, pool, vgi_sqlite::VgiAttachFunc,
                                    nullptr, nullptr);
}
