// Generated-request compatibility surface for the nanoarrow C schemas.
// The authoritative layouts are vgi-c's generated VgiSchema* factories;
// these helpers only fill the five outer records used by this client.
#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "wire/nanoarrow_types.h"

namespace vgi::generated {

inline vgi_sqlite::nano::BatchPtr OneBinary(const char* schema_name,
                                             const std::vector<uint8_t>& request) {
    auto batch = vgi_sqlite::nano::Batch::Named(schema_name);
    batch->SetBytes("request", request);
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildAggregateBindParams(const std::vector<uint8_t>& request) {
    return OneBinary("AggregateBindParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildAggregateDestructorParams(const std::vector<uint8_t>& request) {
    return OneBinary("AggregateDestructorParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildAggregateFinalizeParams(const std::vector<uint8_t>& request) {
    return OneBinary("AggregateFinalizeParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildAggregateUpdateParams(const std::vector<uint8_t>& request) {
    return OneBinary("AggregateUpdateParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildBindParams(const std::vector<uint8_t>& request) {
    return OneBinary("BindParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogAttachParams(const std::vector<uint8_t>& request) {
    return OneBinary("CatalogAttachParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildInitParams(const std::vector<uint8_t>& request) {
    return OneBinary("InitParams", request);
}
inline vgi_sqlite::nano::BatchPtr BuildTableFunctionPlanParams(const std::vector<uint8_t>& request) {
    return OneBinary("TableFunctionPlanParams", request);
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogCatalogsParams() {
    auto batch = vgi_sqlite::nano::Batch::Named("CatalogCatalogsParams");
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogTransactionBeginParams(
    const std::vector<uint8_t>& attach) {
    auto batch = vgi_sqlite::nano::Batch::Named("CatalogTransactionBeginParams");
    batch->SetBytes("attach_opaque_data", attach);
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogTransactionParams(
    const char* schema_name, const std::vector<uint8_t>& attach,
    const std::vector<uint8_t>& transaction) {
    auto batch = vgi_sqlite::nano::Batch::Named(schema_name);
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->FinishRow();
    return batch;
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTransactionCommitParams(
    const std::vector<uint8_t>& attach, const std::vector<uint8_t>& transaction) {
    return BuildCatalogTransactionParams("CatalogTransactionCommitParams", attach, transaction);
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTransactionRollbackParams(
    const std::vector<uint8_t>& attach, const std::vector<uint8_t>& transaction) {
    return BuildCatalogTransactionParams("CatalogTransactionRollbackParams", attach, transaction);
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogSchemasParams(
    const std::vector<uint8_t>& attach,
    const std::optional<std::vector<uint8_t>>& transaction) {
    auto batch = vgi_sqlite::nano::Batch::Named("CatalogSchemasParams");
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogSchemaContentsTablesParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& path,
    const std::optional<std::vector<uint8_t>>& transaction) {
    auto batch = vgi_sqlite::nano::Batch::Named("CatalogSchemaContentsTablesParams");
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetStringList("path", path);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogSchemaContentsFunctionsParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& path,
    const std::string& type, const std::optional<std::vector<uint8_t>>& transaction) {
    auto batch = vgi_sqlite::nano::Batch::Named("CatalogSchemaContentsFunctionsParams");
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetStringList("path", path);
    batch->SetString("type", type);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogTableReadParams(
    const char* schema_name, const std::vector<uint8_t>& attach,
    const std::vector<std::string>& schema_path, const std::string& name,
    const std::optional<std::string>& at_unit, const std::optional<std::string>& at_value,
    const std::optional<std::vector<uint8_t>>& transaction) {
    auto batch = vgi_sqlite::nano::Batch::Named(schema_name);
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetStringList("schema_path", schema_path);
    batch->SetString("name", name);
    batch->SetString("at_unit", at_unit);
    batch->SetString("at_value", at_value);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->FinishRow();
    return batch;
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTableGetParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& schema_path,
    const std::string& name, const std::optional<std::string>& at_unit,
    const std::optional<std::string>& at_value,
    const std::optional<std::vector<uint8_t>>& transaction) {
    return BuildCatalogTableReadParams("CatalogTableGetParams", attach, schema_path, name,
                                       at_unit, at_value, transaction);
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTableScanFunctionGetParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& schema_path,
    const std::string& name, const std::optional<std::string>& at_unit,
    const std::optional<std::string>& at_value,
    const std::optional<std::vector<uint8_t>>& transaction) {
    return BuildCatalogTableReadParams("CatalogTableScanFunctionGetParams", attach, schema_path,
                                       name, at_unit, at_value, transaction);
}

inline vgi_sqlite::nano::BatchPtr BuildCatalogTableWriteFunctionParams(
    const char* schema_name, const std::vector<uint8_t>& attach,
    const std::vector<std::string>& schema_path, const std::string& name,
    const std::optional<std::vector<uint8_t>>& transaction) {
    auto batch = vgi_sqlite::nano::Batch::Named(schema_name);
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetStringList("schema_path", schema_path);
    batch->SetString("name", name);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->FinishRow();
    return batch;
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTableUpdateFunctionGetParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& schema_path,
    const std::string& name, const std::optional<std::vector<uint8_t>>& transaction) {
    return BuildCatalogTableWriteFunctionParams("CatalogTableUpdateFunctionGetParams", attach,
                                                 schema_path, name, transaction);
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTableDeleteFunctionGetParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& schema_path,
    const std::string& name, const std::optional<std::vector<uint8_t>>& transaction) {
    return BuildCatalogTableWriteFunctionParams("CatalogTableDeleteFunctionGetParams", attach,
                                                 schema_path, name, transaction);
}
inline vgi_sqlite::nano::BatchPtr BuildCatalogTableInsertFunctionGetParams(
    const std::vector<uint8_t>& attach, const std::vector<std::string>& schema_path,
    const std::string& name, const std::optional<std::vector<uint8_t>>& transaction,
    const std::optional<std::string>& writable_branch_function_name) {
    auto batch = vgi_sqlite::nano::Batch::Named("CatalogTableInsertFunctionGetParams");
    batch->SetBytes("attach_opaque_data", attach);
    batch->SetStringList("schema_path", schema_path);
    batch->SetString("name", name);
    batch->SetBytes("transaction_opaque_data", transaction);
    batch->SetString("writable_branch_function_name", writable_branch_function_name);
    batch->FinishRow();
    return batch;
}

inline vgi_sqlite::nano::BatchPtr BuildClientCapabilities(
    const std::string& engine, const std::vector<std::string>& native_formats,
    const std::vector<std::string>& catalogs, bool can_stream,
    const std::vector<std::string>& filter_encodings,
    const std::optional<std::vector<uint8_t>>& catalog, bool query_pushdown = false) {
    auto batch = vgi_sqlite::nano::Batch::Named("ClientCapabilities");
    batch->SetString("engine", engine);
    batch->SetStringList("native_formats", native_formats);
    batch->SetStringList("catalogs", catalogs);
    batch->SetBool("can_stream", can_stream);
    batch->SetStringList("filter_encodings", filter_encodings);
    batch->SetBytes("catalog", catalog);
    if (query_pushdown)
        throw std::runtime_error("query_pushdown builder is not implemented by vgi-sqlite");
    batch->SetNullStruct("query_pushdown");
    batch->FinishRow();
    return batch;
}

}  // namespace vgi::generated
