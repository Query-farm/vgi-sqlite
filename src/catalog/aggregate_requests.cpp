// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/aggregate_requests.h"

#include <stdexcept>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "wire/wire_builders.h"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {

void check_ok(const arrow::Status& status, const char* what) {
    if (!status.ok()) throw std::runtime_error(std::string(what) + ": " + status.ToString());
}

template <typename BuilderType>
std::shared_ptr<arrow::Array> finish(BuilderType& builder, const char* what) {
    auto result = builder.Finish();
    if (!result.ok()) throw std::runtime_error(std::string("finishing ") + what + ": " + result.status().ToString());
    return result.ValueUnsafe();
}

std::shared_ptr<arrow::Array> binary_required(const std::vector<uint8_t>& value) {
    arrow::BinaryBuilder b;
    check_ok(b.Append(value.data(), static_cast<int32_t>(value.size())), "append binary");
    return finish(b, "binary");
}

std::shared_ptr<arrow::Array> optional_binary_or_null(const std::optional<std::vector<uint8_t>>& v) {
    arrow::BinaryBuilder b;
    if (v) {
        check_ok(b.Append(v->data(), static_cast<int32_t>(v->size())), "append binary");
    } else {
        check_ok(b.AppendNull(), "append null binary");
    }
    return finish(b, "optional binary");
}

}  // namespace

std::shared_ptr<arrow::RecordBatch> BuildAggregateBindRequest(
    const std::string& function_name, const std::vector<uint8_t>& arguments_ipc_bytes,
    const std::optional<std::vector<uint8_t>>& input_schema_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto schema = arrow::schema({
        arrow::field("function_name", arrow::utf8(), false),
        arrow::field("arguments", arrow::binary(), false),
        arrow::field("input_schema", arrow::binary(), true),
        arrow::field("settings", arrow::binary(), true),
        arrow::field("secrets", arrow::binary(), true),
        arrow::field("attach_opaque_data", arrow::binary(), true),
        arrow::field("schema_name", arrow::utf8(), true),
    });
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        vgi::BuildStringScalar(function_name),
        binary_required(arguments_ipc_bytes),
        optional_binary_or_null(input_schema_bytes),
        optional_binary_or_null(std::nullopt),  // settings
        optional_binary_or_null(std::nullopt),  // secrets
        optional_binary_or_null(attach_opaque_data),
        vgi::BuildOptionalStringScalar(schema_name),
    };
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

AggregateBindResponseResult ParseAggregateBindResponse(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) throw std::runtime_error("empty AggregateBindResponse");
    AggregateBindResponseResult result;

    auto schema_col = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("output_schema"));
    if (!schema_col || schema_col->IsNull(0)) throw std::runtime_error("AggregateBindResponse missing output_schema");
    auto view = schema_col->GetView(0);
    auto buffer = arrow::Buffer::Wrap(view.data(), view.size());
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo dict_memo;
    auto schema_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
    check_ok(schema_result.status(), "deserializing AggregateBindResponse.output_schema");
    result.output_schema = schema_result.ValueUnsafe();

    auto exec_col = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("execution_id"));
    if (!exec_col || exec_col->IsNull(0)) throw std::runtime_error("AggregateBindResponse missing execution_id");
    auto exec_view = exec_col->GetView(0);
    result.execution_id.assign(exec_view.data(), exec_view.data() + exec_view.size());
    return result;
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateUpdateRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& input_batch_ipc_bytes, const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto schema = arrow::schema({
        arrow::field("function_name", arrow::utf8(), false),
        arrow::field("execution_id", arrow::binary(), false),
        arrow::field("input_batch", arrow::binary(), false),
        arrow::field("attach_opaque_data", arrow::binary(), true),
        arrow::field("schema_name", arrow::utf8(), true),
    });
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        vgi::BuildStringScalar(function_name),
        binary_required(execution_id),
        binary_required(input_batch_ipc_bytes),
        optional_binary_or_null(attach_opaque_data),
        vgi::BuildOptionalStringScalar(schema_name),
    };
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateFinalizeRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& group_ids_batch_ipc_bytes, const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data, const std::optional<std::string>& schema_name) {
    auto schema = arrow::schema({
        arrow::field("function_name", arrow::utf8(), false),
        arrow::field("execution_id", arrow::binary(), false),
        arrow::field("group_ids_batch", arrow::binary(), false),
        arrow::field("output_schema", arrow::binary(), false),
        arrow::field("attach_opaque_data", arrow::binary(), true),
        arrow::field("schema_name", arrow::utf8(), true),
    });
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        vgi::BuildStringScalar(function_name),
        binary_required(execution_id),
        binary_required(group_ids_batch_ipc_bytes),
        binary_required(output_schema_bytes),
        optional_binary_or_null(attach_opaque_data),
        vgi::BuildOptionalStringScalar(schema_name),
    };
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> ParseAggregateFinalizeResponse(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) throw std::runtime_error("empty AggregateFinalizeResponse");
    auto result_col = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("result_batch"));
    if (!result_col || result_col->IsNull(0)) throw std::runtime_error("AggregateFinalizeResponse missing result_batch");
    auto view = result_col->GetView(0);
    std::string ipc_bytes(view.data(), view.size());
    auto decoded = wire::decode_ipc(ipc_bytes);
    if (!decoded) throw std::runtime_error("AggregateFinalizeResponse.result_batch failed to decode");
    return decoded;
}

std::shared_ptr<arrow::RecordBatch> BuildAggregateDestructorRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& group_ids_batch_ipc_bytes, const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::string>& schema_name) {
    auto schema = arrow::schema({
        arrow::field("function_name", arrow::utf8(), false),
        arrow::field("execution_id", arrow::binary(), false),
        arrow::field("group_ids_batch", arrow::binary(), false),
        arrow::field("attach_opaque_data", arrow::binary(), true),
        arrow::field("schema_name", arrow::utf8(), true),
    });
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
        vgi::BuildStringScalar(function_name),
        binary_required(execution_id),
        binary_required(group_ids_batch_ipc_bytes),
        optional_binary_or_null(attach_opaque_data),
        vgi::BuildOptionalStringScalar(schema_name),
    };
    return arrow::RecordBatch::Make(schema, 1, arrays);
}

}  // namespace vgi_sqlite
