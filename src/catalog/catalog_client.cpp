// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/catalog_client.h"

#include <utility>

#include "catalog/catalog_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

// Every non-void RPC answers the same one-column outer envelope,
// {result: binary}; for a Result-kind method (every method this client
// calls, so far) those bytes are a self-describing IPC stream of the
// method's *ResultSchema() batch. call_unary() itself throws
// vgi_rpc::RpcException on a protocol-level error response, so a normal
// return here means a well-formed result.
std::shared_ptr<arrow::RecordBatch> Call(VgiConnection& connection, const std::string& method,
                                          const std::shared_ptr<arrow::RecordBatch>& params) {
    auto response = connection.client().call_unary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// Unwrap a {items: list<binary>} result into each item's decoded batch.
std::vector<std::shared_ptr<arrow::RecordBatch>> Items(
    const std::shared_ptr<arrow::RecordBatch>& result) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> items;
    for (const auto& raw : wire::get_binary_list(result, "items")) {
        if (auto decoded = wire::decode_ipc(raw)) items.push_back(std::move(decoded));
    }
    return items;
}

CatalogTable ParseTableInfo(const std::string& schema_name_fallback,
                            const std::shared_ptr<arrow::RecordBatch>& info) {
    CatalogTable table;
    table.name = wire::get_string(info, "name");
    table.schema_name = wire::get_optional_string(info, "schema_name").value_or(schema_name_fallback);
    table.comment = wire::get_optional_string(info, "comment");
    table.columns = wire::get_schema(info, "columns");
    table.supports_insert = wire::get_bool(info, "supports_insert");
    table.supports_update = wire::get_bool(info, "supports_update");
    table.supports_delete = wire::get_bool(info, "supports_delete");
    if (auto v = wire::get_optional_int64(info, "cardinality_estimate")) table.cardinality_estimate = *v;
    if (auto v = wire::get_optional_int64(info, "cardinality_max")) table.cardinality_max = *v;
    if (auto scan_fn = wire::get_ipc(info, "scan_function")) table.scan_function = ParseScanFunction(scan_fn);
    return table;
}

}  // namespace

ScanFunction ParseScanFunction(const std::shared_ptr<arrow::RecordBatch>& scan_function_info) {
    ScanFunction fn;
    fn.function_name = wire::get_string(scan_function_info, "function_name");
    fn.arguments_ipc_bytes = to_bytes(wire::get_binary(scan_function_info, "arguments"));
    return fn;
}

CatalogAttachResult VgiCatalogClient::Attach(const std::string& catalog_name,
                                              const std::optional<std::string>& data_version_spec,
                                              const std::optional<std::string>& implementation_version) {
    auto inner = BuildCatalogAttachRequest(catalog_name, /*options_ipc_bytes=*/std::nullopt,
                                            data_version_spec, implementation_version);
    auto params = gen::BuildCatalogAttachParams(to_bytes(wire::encode_ipc(inner)));
    auto result = Call(connection_, "catalog_attach", params);

    CatalogAttachResult attach;
    attach.attach_opaque_data = wire::get_binary(result, "attach_opaque_data");
    attach.supports_transactions = wire::get_bool(result, "supports_transactions");
    attach.supports_time_travel = wire::get_bool(result, "supports_time_travel");
    attach.default_schema = wire::get_string(result, "default_schema");
    attach.resolved_data_version = wire::get_optional_string(result, "resolved_data_version");
    attach.resolved_implementation_version =
        wire::get_optional_string(result, "resolved_implementation_version");
    return attach;
}

std::vector<std::string> VgiCatalogClient::Catalogs() {
    auto params = gen::BuildCatalogCatalogsParams();
    auto result = Call(connection_, "catalog_catalogs", params);
    std::vector<std::string> names;
    for (const auto& item : Items(result)) names.push_back(wire::get_string(item, "name"));
    return names;
}

std::vector<CatalogSchema> VgiCatalogClient::Schemas(const std::string& attach_opaque_data) {
    auto params = gen::BuildCatalogSchemasParams(to_bytes(attach_opaque_data),
                                                  /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_schemas", params);
    std::vector<CatalogSchema> schemas;
    for (const auto& item : Items(result)) {
        CatalogSchema schema;
        schema.name = wire::get_string(item, "name");
        schema.comment = wire::get_optional_string(item, "comment");
        schemas.push_back(std::move(schema));
    }
    return schemas;
}

std::vector<CatalogTable> VgiCatalogClient::SchemaContentsTables(const std::string& attach_opaque_data,
                                                                  const std::string& schema_name) {
    auto params = gen::BuildCatalogSchemaContentsTablesParams(
        to_bytes(attach_opaque_data), schema_name, /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_schema_contents_tables", params);
    std::vector<CatalogTable> tables;
    for (const auto& item : Items(result)) tables.push_back(ParseTableInfo(schema_name, item));
    return tables;
}

CatalogTable VgiCatalogClient::TableGet(const std::string& attach_opaque_data,
                                        const std::string& schema_name,
                                        const std::string& table_name) {
    auto params = gen::BuildCatalogTableGetParams(to_bytes(attach_opaque_data), schema_name, table_name,
                                                   /*at_unit=*/std::nullopt, /*at_value=*/std::nullopt,
                                                   /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_table_get", params);
    auto items = Items(result);
    if (items.empty()) {
        throw std::runtime_error("catalog_table_get: no such table '" + schema_name + "." +
                                  table_name + "'");
    }
    auto table = ParseTableInfo(schema_name, items.front());
    if (!table.scan_function) {
        table.scan_function = TableScanFunctionGet(attach_opaque_data, schema_name, table_name);
    }
    return table;
}

ScanFunction VgiCatalogClient::TableScanFunctionGet(const std::string& attach_opaque_data,
                                                     const std::string& schema_name,
                                                     const std::string& table_name) {
    auto params = gen::BuildCatalogTableScanFunctionGetParams(
        to_bytes(attach_opaque_data), schema_name, table_name, /*at_unit=*/std::nullopt,
        /*at_value=*/std::nullopt, /*transaction_opaque_data=*/std::nullopt);
    // catalog_table_scan_function_get is a Binary-kind method: the outer
    // envelope's "result" bytes are the ScanFunctionResult payload
    // directly - mechanically identical to Call()'s Result-kind unwrap
    // (both are "binary field holding one IPC stream"), so it's reused.
    auto result = Call(connection_, "catalog_table_scan_function_get", params);
    return ParseScanFunction(result);
}

ScanFunction VgiCatalogClient::TableInsertFunctionGet(const std::string& attach_opaque_data,
                                                       const std::string& schema_name,
                                                       const std::string& table_name) {
    auto params = gen::BuildCatalogTableInsertFunctionGetParams(
        to_bytes(attach_opaque_data), schema_name, table_name,
        /*transaction_opaque_data=*/std::nullopt, /*writable_branch_function_name=*/std::nullopt);
    auto result = Call(connection_, "catalog_table_insert_function_get", params);
    return ParseScanFunction(result);
}

ScanFunction VgiCatalogClient::TableUpdateFunctionGet(const std::string& attach_opaque_data,
                                                       const std::string& schema_name,
                                                       const std::string& table_name) {
    auto params = gen::BuildCatalogTableUpdateFunctionGetParams(
        to_bytes(attach_opaque_data), schema_name, table_name,
        /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_table_update_function_get", params);
    return ParseScanFunction(result);
}

ScanFunction VgiCatalogClient::TableDeleteFunctionGet(const std::string& attach_opaque_data,
                                                       const std::string& schema_name,
                                                       const std::string& table_name) {
    auto params = gen::BuildCatalogTableDeleteFunctionGetParams(
        to_bytes(attach_opaque_data), schema_name, table_name,
        /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_table_delete_function_get", params);
    return ParseScanFunction(result);
}

namespace {
std::vector<CatalogFunction> SchemaContentsFunctionsByType(VgiConnection& connection,
                                                            const std::string& attach_opaque_data,
                                                            const std::string& schema_name,
                                                            const std::string& type) {
    auto params = gen::BuildCatalogSchemaContentsFunctionsParams(to_bytes(attach_opaque_data), schema_name, type,
                                                                  /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection, "catalog_schema_contents_functions", params);
    std::vector<CatalogFunction> functions;
    for (const auto& item : Items(result)) {
        CatalogFunction fn;
        fn.name = wire::get_string(item, "name");
        fn.schema_name = wire::get_optional_string(item, "schema_name").value_or(schema_name);
        fn.argument_types = wire::get_schema(item, "arguments");
        functions.push_back(std::move(fn));
    }
    return functions;
}
}  // namespace

std::vector<CatalogFunction> VgiCatalogClient::SchemaContentsScalarFunctions(
    const std::string& attach_opaque_data, const std::string& schema_name) {
    return SchemaContentsFunctionsByType(connection_, attach_opaque_data, schema_name, "SCALAR_FUNCTION");
}

std::vector<CatalogFunction> VgiCatalogClient::SchemaContentsAggregateFunctions(
    const std::string& attach_opaque_data, const std::string& schema_name) {
    return SchemaContentsFunctionsByType(connection_, attach_opaque_data, schema_name, "AGGREGATE_FUNCTION");
}

}  // namespace vgi_sqlite
