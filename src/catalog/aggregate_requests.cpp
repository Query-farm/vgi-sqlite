// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/aggregate_requests.h"

#include <stdexcept>

#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {

void SetSchemaPath(const nano::BatchPtr& batch, const std::optional<std::string>& schema_name) {
    batch->SetStringList("schema_path", schema_name ? std::optional<std::vector<std::string>>{{*schema_name}}
                                                     : std::nullopt);
}

}  // namespace

nano::BatchPtr BuildAggregateBindRequest(
    const std::string& function_name, const std::vector<uint8_t>& arguments_ipc_bytes,
    const std::optional<std::vector<uint8_t>>& input_schema_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto batch = nano::Batch::Named("AggregateBindRequest");
    batch->SetString("function_name", function_name);
    batch->SetBytes("arguments", arguments_ipc_bytes);
    batch->SetBytes("input_schema", input_schema_bytes);
    batch->SetNull("settings");
    batch->SetNull("secrets");
    batch->SetBytes("attach_opaque_data", attach_opaque_data);
    SetSchemaPath(batch, schema_name);
    batch->SetStringList("argument_names", std::nullopt);
    batch->FinishRow();
    return batch;
}

AggregateBindResponseResult ParseAggregateBindResponse(const nano::BatchPtr& batch) {
    if (!batch || !batch->num_rows()) throw std::runtime_error("empty AggregateBindResponse");
    auto schema_bytes = wire::get_optional_binary(batch, "output_schema");
    auto execution = wire::get_optional_binary(batch, "execution_id");
    if (!schema_bytes) throw std::runtime_error("AggregateBindResponse missing output_schema");
    if (!execution) throw std::runtime_error("AggregateBindResponse missing execution_id");
    AggregateBindResponseResult result;
    result.output_schema = wire::decode_schema(*schema_bytes);
    result.execution_id.assign(execution->begin(), execution->end());
    return result;
}

nano::BatchPtr BuildAggregateUpdateRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& input_batch_ipc_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto batch = nano::Batch::Named("AggregateUpdateRequest");
    batch->SetString("function_name", function_name);
    batch->SetBytes("execution_id", execution_id);
    batch->SetBytes("input_batch", input_batch_ipc_bytes);
    batch->SetBytes("attach_opaque_data", attach_opaque_data);
    SetSchemaPath(batch, schema_name);
    batch->FinishRow();
    return batch;
}

nano::BatchPtr BuildAggregateFinalizeRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& group_ids_batch_ipc_bytes,
    const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto batch = nano::Batch::Named("AggregateFinalizeRequest");
    batch->SetString("function_name", function_name);
    batch->SetBytes("execution_id", execution_id);
    batch->SetBytes("group_ids_batch", group_ids_batch_ipc_bytes);
    batch->SetBytes("output_schema", output_schema_bytes);
    batch->SetBytes("attach_opaque_data", attach_opaque_data);
    SetSchemaPath(batch, schema_name);
    batch->FinishRow();
    return batch;
}

nano::BatchPtr ParseAggregateFinalizeResponse(const nano::BatchPtr& batch) {
    if (!batch || !batch->num_rows()) throw std::runtime_error("empty AggregateFinalizeResponse");
    auto result = wire::get_optional_binary(batch, "result_batch");
    if (!result) throw std::runtime_error("AggregateFinalizeResponse missing result_batch");
    auto decoded = wire::decode_ipc(*result);
    if (!decoded) throw std::runtime_error("AggregateFinalizeResponse.result_batch failed to decode");
    return decoded;
}

nano::BatchPtr BuildAggregateDestructorRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& group_ids_batch_ipc_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto batch = nano::Batch::Named("AggregateDestructorRequest");
    batch->SetString("function_name", function_name);
    batch->SetBytes("execution_id", execution_id);
    batch->SetBytes("group_ids_batch", group_ids_batch_ipc_bytes);
    batch->SetBytes("attach_opaque_data", attach_opaque_data);
    SetSchemaPath(batch, schema_name);
    batch->FinishRow();
    return batch;
}

}  // namespace vgi_sqlite
