// © Copyright 2026 Query Farm LLC - https://query.farm
//
// VgiCatalogClient: issues the catalog-discovery RPCs (catalog_attach,
// catalog_catalogs, catalog_schemas, catalog_schema_contents_tables,
// catalog_table_get) over a VgiConnection and parses their responses into
// plain client-side structs.
//
// Every non-void RPC answers the same one-column outer envelope,
// {result: binary} (see src/wire/wire_readers.h's file comment) - Call()
// does that unwrap once so callers work directly with each method's
// *ResultSchema() batch.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

#include "rpc/vgi_connection.h"

namespace vgi_sqlite {

// Which table function to bind/init/scan to read a table's rows, and the
// pre-encoded (fixed, worker-chosen) arguments to bind it with. A VGI
// "table" doesn't scan itself - TableInfo.scan_function names the actual
// scan; decoded from ScanFunctionResultSchema.
struct ScanFunction {
    std::string function_name;
    std::vector<uint8_t> arguments_ipc_bytes;
    // FunctionInfo.supports_splits - opt-in capability, defaults false for
    // both an older worker that predates the field and an ordinary
    // non-split function (see catalog_client.cpp's ParseScanFunction: read
    // via get_optional_bool, not get_bool, specifically so an absent
    // column - not just an absent/null value - degrades to "no splits"
    // instead of a parse error). When true, TableScanner plans the scan
    // into independently-redeemable splits (catalog_table_plan.h) instead
    // of the ordinary single whole-scan init - see that file's comment for
    // the full design and the protocol source this was built against.
    bool supports_splits = false;
    // When true, arguments_ipc_bytes is ALREADY a wrapped one-row
    // {args: struct<...>} batch (VGI's BindRequest.arguments wire shape
    // directly - "positional_N"/"named_<name>" struct fields) and
    // TableScanner::Bind() must send it as-is instead of running it
    // through WrapAsArgsStruct, which unconditionally renames every
    // column "positional_N" by index - correct for every real table's
    // inlined scan_function today (all observed so far bind purely
    // positionally), but wrong for a function whose worker-side argument
    // resolution keys by declared NAME (`Annotated[int, Arg("n", ...)]`-
    // style, confirmed against vgi-python's splits fixtures - see
    // tools/split_probe.cpp, the only place this is set true today).
    // Always false for every catalog-derived ScanFunction (ParseScanFunction
    // never sets it) - named-argument table functions aren't otherwise
    // supported by this driver yet, same documented gap as
    // ScalarFunctionCaller's own "positional arguments only" scope.
    bool arguments_already_wrapped = false;
};

ScanFunction ParseScanFunction(const std::shared_ptr<arrow::RecordBatch>& scan_function_info);

// A worker table, as much of TableInfo as the read-only scan path needs.
// Deliberately trimmed from vgi-c++'s worker-authoring CatalogTable
// (include/vgi/catalog.h): no branches/inline_scan - those are worker-
// internal wiring a client never populates.
struct CatalogTable {
    std::string name;
    std::string schema_name;
    std::optional<std::string> comment;
    // The table's Arrow schema (columns), decoded from TableInfo.columns.
    std::shared_ptr<arrow::Schema> columns;
    bool supports_insert = false;
    bool supports_update = false;
    bool supports_delete = false;
    int64_t cardinality_estimate = -1;  // -1 = unknown
    int64_t cardinality_max = -1;       // -1 = unknown
    // Present when TableInfo inlined it (the common case - skips a
    // separate catalog_table_scan_function_get round trip). Falling back
    // to that RPC when absent is not yet implemented (all fixture tables
    // observed so far inline it).
    std::optional<ScanFunction> scan_function;
};

struct CatalogSchema {
    std::string name;
    std::optional<std::string> comment;
};

// A table_in_out function eligible for this driver's per-row-correlated
// table-valued-function mapping (see vtab/vgi_table_in_out_vtab.h's file
// comment for the full design). Only the "blended"/RowTransformFunction
// shape is representable in SQLite at all - FunctionInfo.input_from_args
// true means the function's declared positional arguments ARE its
// per-row input columns (no separate relation-valued/TABLE-typed
// argument, which SQLite's table-valued-function calling convention has
// no way to express - see VgiCatalogClient::SchemaContentsTableInOutFunctions'
// file comment on why that shape is filtered out at discovery, not
// merely undocumented).
struct CatalogTableInOutFunction {
    std::string function_name;
    std::string schema_name;
    // Declared argument names+types (FunctionInfo.arguments, a SCHEMA,
    // not values - same "arguments describes types, not literal call
    // values" shape CatalogFunction::argument_types already uses for
    // scalar/aggregate functions). Field order is positional-call order;
    // field names become this function's HIDDEN column names.
    std::shared_ptr<arrow::Schema> input_schema;
    // FunctionInfo.projection_pushdown - not yet acted on by this driver
    // (v1 always requests every output column; see
    // vgi_table_in_out_vtab.cpp's file comment on why that's scoped out
    // for now), but decoded and carried through so a future projection
    // pushdown pass doesn't need to re-thread it from scratch.
    bool projection_pushdown = false;
};

// A worker's registered function, as much of FunctionInfo as calling a
// plain (non-const-argument) scalar function needs.
struct CatalogFunction {
    std::string name;
    std::string schema_name;  // where the function is *registered* - not
                               // necessarily the same schema as any table
                               // that happens to use it (see vgi_vtab.cpp's
                               // xFilter comment on filter_echo_table).
    // One field per positional argument, decoded from FunctionInfo.arguments
    // (itself a serialized Arrow schema, not values - distinct from
    // ScanFunctionResultSchema.arguments' flat values batch).
    std::shared_ptr<arrow::Schema> argument_types;
};

// Result of catalog_attach: the opaque session token every later call on
// this attachment must echo back, plus the worker's advertised
// capabilities.
struct CatalogAttachResult {
    std::string attach_opaque_data;
    bool supports_transactions = false;
    bool supports_time_travel = false;
    std::string default_schema;
    std::optional<std::string> resolved_data_version;
    std::optional<std::string> resolved_implementation_version;
};

class VgiCatalogClient {
public:
    explicit VgiCatalogClient(VgiConnection& connection) : connection_(connection) {}

    // ATTACH: resolves `catalog_name` against the worker (which may serve
    // more than one catalog) and returns the session token every later RPC
    // on this attachment must carry.
    CatalogAttachResult Attach(const std::string& catalog_name,
                                const std::optional<std::string>& data_version_spec = std::nullopt,
                                const std::optional<std::string>& implementation_version = std::nullopt);

    // The names of every catalog this worker can serve (vgi_catalogs()'s
    // data source; also how vgi_attach() disambiguates a bare worker
    // location that serves more than one).
    std::vector<std::string> Catalogs();

    std::vector<CatalogSchema> Schemas(const std::string& attach_opaque_data);

    std::vector<CatalogTable> SchemaContentsTables(const std::string& attach_opaque_data,
                                                    const std::string& schema_name);

    CatalogTable TableGet(const std::string& attach_opaque_data, const std::string& schema_name,
                          const std::string& table_name);

    // Fallback for a table whose TableInfo didn't inline scan_function
    // (CatalogTable::scan_function is nullopt).
    ScanFunction TableScanFunctionGet(const std::string& attach_opaque_data,
                                       const std::string& schema_name, const std::string& table_name);

    // Every scalar function registered in `schema_name` - vgi_attach()'s
    // data source for registering native SQLite functions.
    std::vector<CatalogFunction> SchemaContentsScalarFunctions(const std::string& attach_opaque_data,
                                                                 const std::string& schema_name);

    // Every aggregate function registered in `schema_name` - vgi_attach()'s
    // data source for registering native SQLite aggregate functions. Same
    // catalog_schema_contents_functions RPC as
    // SchemaContentsScalarFunctions, filtered by type="AGGREGATE_FUNCTION"
    // instead of "SCALAR_FUNCTION" - not a separate listing RPC.
    std::vector<CatalogFunction> SchemaContentsAggregateFunctions(const std::string& attach_opaque_data,
                                                                    const std::string& schema_name);

    // Every table_in_out function registered in `schema_name` that this
    // driver can represent (see CatalogTableInOutFunction's file
    // comment: input_from_args=true, has_finalize=false - every other
    // shape, including a plain no-input table function like
    // split_sequence, is silently excluded here rather than filtered by
    // the caller, since "callable with no correlation at all" isn't
    // representable as a vgi_worker-style fixed table either and has no
    // home in this driver yet - see the plan file's table-function-call
    // gap). Same catalog_schema_contents_functions RPC as the scalar/
    // aggregate listings above, filtered by type="TABLE_FUNCTION" instead -
    // not a separate listing RPC (mirrors SchemaContentsAggregateFunctions'
    // own comment). vgi_attach()'s data source for registering each
    // qualifying function as a vgi_table_in_out virtual table.
    std::vector<CatalogTableInOutFunction> SchemaContentsTableInOutFunctions(
        const std::string& attach_opaque_data, const std::string& schema_name);

    // Fallback for a vgi_table_in_out vtab's xConnect/xCreate, which only
    // has (schema, function) from its own module arguments, not the full
    // listing already enumerated at vgi_attach() time - re-lists and
    // finds by name (there is no dedicated single-function catalog RPC;
    // vgi's own DuckDB client resolves the same way, see the plan file's
    // Milestone 9 research notes). Throws if no such eligible function is
    // found (a real "no such table_in_out function" error, not a null/
    // optional - matches VgiCatalogClient::TableGet's own contract for
    // an unknown table).
    CatalogTableInOutFunction TableInOutFunctionGet(const std::string& attach_opaque_data,
                                                     const std::string& schema_name,
                                                     const std::string& function_name);

    // Write-path function resolution - only called for a table whose
    // CatalogTable::supports_insert/update/delete says the operation is
    // available. `WriteFunctionResult` is wire-identical to
    // `ScanFunctionResult` (confirmed by reading vgi-c++'s
    // VgiWriteFunctionResult, a literal type alias for VgiScanFunctionResult
    // - not documented anywhere read ahead of time), so ScanFunction/
    // ParseScanFunction are reused rather than duplicated. Each is a
    // Binary-kind method like TableScanFunctionGet, not a Result-kind one.
    ScanFunction TableInsertFunctionGet(const std::string& attach_opaque_data,
                                         const std::string& schema_name, const std::string& table_name);
    // Transaction lifecycle - only meaningful for a catalog whose
    // CatalogAttachResult.supports_transactions is true; see
    // connection_pool.h's Begin/Commit/RollbackTransaction, which gate on
    // that and coordinate these calls across every vgi_worker table
    // instance sharing one (location, catalog) transaction (SQLite calls
    // xBegin/xCommit/xRollback once per vtab instance, VGI's transaction
    // RPCs are scoped to the whole attachment - a mismatch this driver's
    // caller resolves, not this client).
    std::optional<std::vector<uint8_t>> TransactionBegin(const std::string& attach_opaque_data);
    void TransactionCommit(const std::string& attach_opaque_data, const std::vector<uint8_t>& transaction_opaque_data);
    void TransactionRollback(const std::string& attach_opaque_data,
                              const std::vector<uint8_t>& transaction_opaque_data);

    ScanFunction TableUpdateFunctionGet(const std::string& attach_opaque_data,
                                         const std::string& schema_name, const std::string& table_name);
    ScanFunction TableDeleteFunctionGet(const std::string& attach_opaque_data,
                                         const std::string& schema_name, const std::string& table_name);

private:
    VgiConnection& connection_;
};

}  // namespace vgi_sqlite
