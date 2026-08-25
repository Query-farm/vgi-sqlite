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

// A worker table, as much of TableInfo as the read-only scan path needs.
// Deliberately trimmed from vgi-c++'s worker-authoring CatalogTable
// (include/vgi/catalog.h): no scan_function/branches/inline_scan - those
// are worker-internal wiring a client never populates, only consumes via
// separate RPCs (catalog_table_scan_function_get) when not inlined.
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
};

struct CatalogSchema {
    std::string name;
    std::optional<std::string> comment;
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

private:
    VgiConnection& connection_;
};

}  // namespace vgi_sqlite
