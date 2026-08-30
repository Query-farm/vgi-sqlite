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
    auto response = connection.CallUnary(method, params);
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
    // get_optional_bool, not get_bool: an older worker predating the
    // splits protocol addition won't send this column at all (not merely
    // a null value) - defaulting to false via the missing-column case is
    // exactly "no splits", not an error. See catalog_client.h's
    // ScanFunction::supports_splits comment.
    fn.supports_splits = wire::get_optional_bool(scan_function_info, "supports_splits").value_or(false);
    fn.schema_name = wire::get_optional_string(scan_function_info, "schema_name");
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

std::optional<std::vector<uint8_t>> VgiCatalogClient::TransactionBegin(const std::string& attach_opaque_data) {
    auto params = gen::BuildCatalogTransactionBeginParams(to_bytes(attach_opaque_data));
    auto result = Call(connection_, "catalog_transaction_begin", params);
    auto opaque = wire::get_optional_binary(result, "transaction_opaque_data");
    if (!opaque) return std::nullopt;
    return to_bytes(*opaque);
}

void VgiCatalogClient::TransactionCommit(const std::string& attach_opaque_data,
                                          const std::vector<uint8_t>& transaction_opaque_data) {
    auto params = gen::BuildCatalogTransactionCommitParams(to_bytes(attach_opaque_data), transaction_opaque_data);
    Call(connection_, "catalog_transaction_commit", params);
}

void VgiCatalogClient::TransactionRollback(const std::string& attach_opaque_data,
                                            const std::vector<uint8_t>& transaction_opaque_data) {
    auto params = gen::BuildCatalogTransactionRollbackParams(to_bytes(attach_opaque_data), transaction_opaque_data);
    Call(connection_, "catalog_transaction_rollback", params);
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

namespace {
// catalog_schema_contents_functions' type="TABLE_FUNCTION" filter returns
// EVERY table-shaped function - plain generator functions (e.g.
// split_sequence, no input relation at all) and table_in_out functions
// (both the classic TABLE-arg shape and the blended/RowTransformFunction
// shape this driver targets) all come back tagged the same
// function_type="table" (confirmed by reading vgi-c++'s
// encode_table_function_info/encode_table_in_out_info - both call the
// shared common_function_info helper with enums::function_type::kTable).
// input_from_args is the only field that distinguishes "this function's
// declared positional args ARE its per-row input columns" (representable
// here) from either a plain no-input generator or a classic TABLE-typed-
// argument function (neither representable - see
// CatalogTableInOutFunction's file comment). has_finalize is checked too,
// defensively: the worker's own resolve_metadata rejects a blended
// function that also sets has_finalize (confirmed via vgi-python's
// table_in_out_function.py), so this should never actually filter
// anything out in practice, but a stale/older worker isn't something to
// trust blindly.
std::vector<CatalogTableInOutFunction> ParseTableInOutFunctions(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& items, const std::string& schema_name_fallback) {
    std::vector<CatalogTableInOutFunction> functions;
    for (const auto& item : items) {
        bool input_from_args = wire::get_optional_bool(item, "input_from_args").value_or(false);
        bool has_finalize = wire::get_optional_bool(item, "has_finalize").value_or(false);
        if (!input_from_args || has_finalize) continue;
        CatalogTableInOutFunction fn;
        fn.function_name = wire::get_string(item, "name");
        fn.schema_name = wire::get_optional_string(item, "schema_name").value_or(schema_name_fallback);
        fn.input_schema = wire::get_schema(item, "arguments");
        fn.projection_pushdown = wire::get_optional_bool(item, "projection_pushdown").value_or(false);
        functions.push_back(std::move(fn));
    }
    return functions;
}
}  // namespace

std::vector<CatalogTableInOutFunction> VgiCatalogClient::SchemaContentsTableInOutFunctions(
    const std::string& attach_opaque_data, const std::string& schema_name) {
    auto params = gen::BuildCatalogSchemaContentsFunctionsParams(to_bytes(attach_opaque_data), schema_name,
                                                                  "TABLE_FUNCTION",
                                                                  /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_schema_contents_functions", params);
    return ParseTableInOutFunctions(Items(result), schema_name);
}

CatalogTableInOutFunction VgiCatalogClient::TableInOutFunctionGet(const std::string& attach_opaque_data,
                                                                   const std::string& schema_name,
                                                                   const std::string& function_name,
                                                                   std::optional<int> arity) {
    for (auto& fn : SchemaContentsTableInOutFunctions(attach_opaque_data, schema_name)) {
        if (fn.function_name != function_name) continue;
        if (arity) {
            int fn_arity = fn.input_schema ? fn.input_schema->num_fields() : 0;
            if (fn_arity != *arity) continue;
        }
        return fn;
    }
    std::string what = "no such table_in_out function '" + schema_name + "." + function_name + "'";
    if (arity) what += " with " + std::to_string(*arity) + " argument(s)";
    throw std::runtime_error(what);
}

namespace {
// True iff any field of `argument_schema` carries `vgi_type: table`
// metadata - the classic table_in_out shape's genuinely relation-valued
// argument marker (vgi-python's argument_spec.py: VGI_TYPE_KEY="vgi_type",
// VGI_TYPE_TABLE="table"). A plain table (generator) function never has
// one; excluding a function that does is what keeps this listing from
// trying to represent the one table_in_out shape SQLite's calling
// convention genuinely can't express (see CatalogPlainTableFunction's
// file comment).
bool HasTableTypedArgument(const std::shared_ptr<arrow::Schema>& argument_schema) {
    if (!argument_schema) return false;
    for (int i = 0; i < argument_schema->num_fields(); ++i) {
        auto metadata = argument_schema->field(i)->metadata();
        if (!metadata) continue;
        auto value = metadata->Get("vgi_type");
        if (value.ok() && *value == "table") return true;
    }
    return false;
}

std::vector<CatalogPlainTableFunction> ParsePlainTableFunctions(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& items, const std::string& schema_name_fallback) {
    std::vector<CatalogPlainTableFunction> functions;
    for (const auto& item : items) {
        bool input_from_args = wire::get_optional_bool(item, "input_from_args").value_or(false);
        bool has_finalize = wire::get_optional_bool(item, "has_finalize").value_or(false);
        if (input_from_args || has_finalize) continue;  // those are CatalogTableInOutFunction's territory
        auto argument_schema = wire::get_schema(item, "arguments");
        if (HasTableTypedArgument(argument_schema)) continue;  // classic table_in_out - not representable
        CatalogPlainTableFunction fn;
        fn.function_name = wire::get_string(item, "name");
        fn.schema_name = wire::get_optional_string(item, "schema_name").value_or(schema_name_fallback);
        fn.argument_schema = argument_schema;
        fn.supports_splits = wire::get_optional_bool(item, "supports_splits").value_or(false);
        functions.push_back(std::move(fn));
    }
    return functions;
}
}  // namespace

std::vector<CatalogPlainTableFunction> VgiCatalogClient::SchemaContentsPlainTableFunctions(
    const std::string& attach_opaque_data, const std::string& schema_name) {
    auto params = gen::BuildCatalogSchemaContentsFunctionsParams(to_bytes(attach_opaque_data), schema_name,
                                                                  "TABLE_FUNCTION",
                                                                  /*transaction_opaque_data=*/std::nullopt);
    auto result = Call(connection_, "catalog_schema_contents_functions", params);
    return ParsePlainTableFunctions(Items(result), schema_name);
}

CatalogPlainTableFunction VgiCatalogClient::PlainTableFunctionGet(const std::string& attach_opaque_data,
                                                                   const std::string& schema_name,
                                                                   const std::string& function_name,
                                                                   std::optional<int> arity) {
    for (auto& fn : SchemaContentsPlainTableFunctions(attach_opaque_data, schema_name)) {
        if (fn.function_name != function_name) continue;
        if (arity) {
            int fn_arity = fn.argument_schema ? fn.argument_schema->num_fields() : 0;
            if (fn_arity != *arity) continue;
        }
        return fn;
    }
    std::string what = "no such table function '" + schema_name + "." + function_name + "'";
    if (arity) what += " with " + std::to_string(*arity) + " argument(s)";
    throw std::runtime_error(what);
}

}  // namespace vgi_sqlite
