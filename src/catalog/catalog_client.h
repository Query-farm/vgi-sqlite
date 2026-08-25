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
