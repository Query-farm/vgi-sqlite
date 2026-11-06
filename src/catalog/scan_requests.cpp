// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/scan_requests.h"

#include <stdexcept>

#include "wire/wire_readers.h"

namespace vgi_sqlite {

nano::BatchPtr BuildBindRequest(
    const std::string& function_name, const std::vector<uint8_t>& arguments_ipc_bytes,
    const std::string& function_type, const std::optional<std::vector<uint8_t>>& input_schema_bytes,
    const std::optional<std::vector<uint8_t>>& settings_bytes,
    const std::optional<std::vector<uint8_t>>& secrets_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::vector<uint8_t>>& transaction_opaque_data,
    bool resolved_secrets_provided, const std::optional<std::string>& schema_name) {
    auto batch = nano::Batch::Named("BindRequest");
    batch->SetString("function_name", function_name);
    batch->SetBytes("arguments", arguments_ipc_bytes);
    batch->SetString("function_type", function_type);
    batch->SetBytes("input_schema", input_schema_bytes);
    batch->SetBytes("settings", settings_bytes);
    batch->SetBytes("secrets", secrets_bytes);
    batch->SetBytes("attach_opaque_data", attach_opaque_data);
    batch->SetBytes("transaction_opaque_data", transaction_opaque_data);
    batch->SetBool("resolved_secrets_provided", resolved_secrets_provided);
    batch->SetString("at_unit", std::nullopt);
    batch->SetString("at_value", std::nullopt);
    batch->SetNullStruct("copy_from");
    batch->SetNullStruct("copy_to");
    batch->SetStringList("schema_path", schema_name ? std::optional<std::vector<std::string>>{{*schema_name}}
                                                     : std::nullopt);
    batch->SetStringList("argument_names", std::nullopt);
    batch->FinishRow();
    return batch;
}

BindResponseResult ParseBindResponse(const nano::BatchPtr& batch) {
    if (!batch || batch->num_rows() == 0) throw std::runtime_error("empty BindResponse");
    auto schema_bytes = wire::get_optional_binary(batch, "output_schema");
    if (!schema_bytes) throw std::runtime_error("BindResponse missing output_schema");
    BindResponseResult result;
    result.output_schema = wire::decode_schema(*schema_bytes);
    if (auto opaque = wire::get_optional_binary(batch, "opaque_data"))
        result.opaque_data.assign(opaque->begin(), opaque->end());
    return result;
}

nano::BatchPtr BuildInitRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& bind_opaque_data,
    const std::vector<int64_t>& projection_ids,
    const std::optional<std::string>& pushdown_filters,
    const std::vector<std::string>& join_keys, std::optional<int64_t> row_limit,
    const std::optional<std::string>& phase,
    const std::vector<std::vector<uint8_t>>& split_tokens) {
    auto batch = nano::Batch::Named("InitRequest");
    batch->SetBytes("bind_call", bind_call_bytes);
    batch->SetBytes("output_schema", output_schema_bytes);
    batch->SetBytes("bind_opaque_data", bind_opaque_data);
    batch->SetInt64List("projection_ids",
                        projection_ids.empty() ? std::nullopt
                                               : std::optional<std::vector<int64_t>>(projection_ids));
    batch->SetBytes("pushdown_filters", pushdown_filters);
    std::optional<std::vector<std::vector<uint8_t>>> join_key_bytes;
    if (!join_keys.empty()) {
        join_key_bytes.emplace();
        for (const auto& key : join_keys)
            join_key_bytes->emplace_back(key.begin(), key.end());
    }
    batch->SetBinaryList("join_keys", join_key_bytes);
    batch->SetBinaryList("split_tokens",
                         split_tokens.empty()
                             ? std::nullopt
                             : std::optional<std::vector<std::vector<uint8_t>>>(split_tokens));
    batch->SetInt("row_limit", row_limit);
    batch->SetString("phase", phase);
    batch->SetNull("finalize_state_id");
    batch->SetNull("execution_id");
    batch->SetNull("init_opaque_data");
    batch->SetNull("substream_id");
    batch->SetString("order_by_column_name", std::nullopt);
    batch->SetString("order_by_direction", std::nullopt);
    batch->SetString("order_by_null_order", std::nullopt);
    batch->SetInt("order_by_limit", std::nullopt);
    batch->SetDouble("tablesample_percentage", std::nullopt);
    batch->SetInt("tablesample_seed", std::nullopt);
    batch->FinishRow();
    return batch;
}

GlobalInitResponseResult ParseGlobalInitResponse(const nano::BatchPtr& batch) {
    if (!batch || batch->num_rows() == 0) throw std::runtime_error("empty GlobalInitResponse");
    GlobalInitResponseResult result;
    if (auto execution = wire::get_optional_binary(batch, "execution_id"))
        result.execution_id.assign(execution->begin(), execution->end());
    if (auto workers = wire::get_optional_int64(batch, "max_workers")) result.max_workers = *workers;
    if (result.max_workers <= 0) result.max_workers = 1;
    if (auto opaque = wire::get_optional_binary(batch, "opaque_data"))
        result.opaque_data.assign(opaque->begin(), opaque->end());
    return result;
}

}  // namespace vgi_sqlite
