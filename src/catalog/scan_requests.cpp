// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/scan_requests.h"

#include <stdexcept>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include "wire/wire_builders.h"

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

std::shared_ptr<arrow::Array> null_binary() {
    arrow::BinaryBuilder b;
    check_ok(b.AppendNull(), "append null binary");
    return finish(b, "null binary");
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

// A 1-row, all-null struct column. Needed because a declared-but-absent
// nested field (bind's copy_from/copy_to on a non-COPY call) must still
// occupy its column: a worker validating its declared parameter contract
// compares field order, name, type AND nullability.
std::shared_ptr<arrow::Array> null_struct(const std::shared_ptr<arrow::DataType>& type) {
    std::unique_ptr<arrow::ArrayBuilder> builder;
    check_ok(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder), "make struct builder");
    check_ok(static_cast<arrow::StructBuilder*>(builder.get())->AppendNull(), "append null struct");
    return finish(*builder, "null struct");
}

// A null dictionary(int16, utf8) array - the codegen'd helpers only cover
// the case where the caller has a value; init's several optional enum
// fields need an explicit null entry when unset.
std::shared_ptr<arrow::Array> null_dictionary(const std::shared_ptr<arrow::DataType>& dict_type,
                                               const std::vector<std::string>& dictionary_values) {
    arrow::Int16Builder index_builder;
    check_ok(index_builder.AppendNull(), "append null dict index");
    auto index_arr = finish(index_builder, "null_dict_index");
    arrow::StringBuilder dict_builder;
    for (const auto& v : dictionary_values) check_ok(dict_builder.Append(v), "append dict value");
    auto dict_arr = finish(dict_builder, "dict_values");
    auto result = arrow::DictionaryArray::FromArrays(dict_type, index_arr, dict_arr);
    check_ok(result.status(), "creating null dictionary array");
    return result.ValueUnsafe();
}

}  // namespace

std::shared_ptr<arrow::RecordBatch> BuildBindRequest(
    const std::string& function_name, const std::vector<uint8_t>& arguments_ipc_bytes,
    const std::string& function_type, const std::optional<std::vector<uint8_t>>& input_schema_bytes,
    const std::optional<std::vector<uint8_t>>& settings_bytes,
    const std::optional<std::vector<uint8_t>>& secrets_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data,
    const std::optional<std::vector<uint8_t>>& transaction_opaque_data, bool resolved_secrets_provided,
    const std::optional<std::string>& schema_name) {
    static const std::vector<std::string> function_type_values = {"SCALAR", "TABLE", "AGGREGATE"};
    // copy_from/copy_to structs: always present, null when this isn't a COPY
    // bind - a worker validating its parameter contract by Schema.Equal
    // rejects a batch missing a declared column outright.
    auto copy_from_type = arrow::struct_({
        arrow::field("format", arrow::utf8(), false),
        arrow::field("file_path", arrow::utf8(), false),
        arrow::field("expected_schema", arrow::binary(), false),
    });
    auto copy_to_type = arrow::struct_({
        arrow::field("format", arrow::utf8(), false),
        arrow::field("file_path", arrow::utf8(), false),
    });

    auto schema = arrow::schema({
        arrow::field("function_name", arrow::utf8(), false),
        arrow::field("arguments", arrow::binary(), false),
        arrow::field("function_type", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
        arrow::field("input_schema", arrow::binary(), true),
        arrow::field("settings", arrow::binary(), true),
        arrow::field("secrets", arrow::binary(), true),
        arrow::field("attach_opaque_data", arrow::binary(), true),
        arrow::field("transaction_opaque_data", arrow::binary(), true),
        arrow::field("resolved_secrets_provided", arrow::boolean(), false),
        arrow::field("at_unit", arrow::utf8(), true),
        arrow::field("at_value", arrow::utf8(), true),
        arrow::field("copy_from", copy_from_type, true),
        arrow::field("copy_to", copy_to_type, true),
        arrow::field("schema_name", arrow::utf8(), true),
    });

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.push_back(vgi::BuildStringScalar(function_name));
    {
        arrow::BinaryBuilder b;
        check_ok(b.Append(arguments_ipc_bytes.data(), static_cast<int32_t>(arguments_ipc_bytes.size())),
                 "append arguments");
        arrays.push_back(finish(b, "arguments"));
    }
    arrays.push_back(vgi::BuildEnumArray(function_type, function_type_values));
    arrays.push_back(optional_binary_or_null(input_schema_bytes));
    arrays.push_back(optional_binary_or_null(settings_bytes));
    arrays.push_back(optional_binary_or_null(secrets_bytes));
    arrays.push_back(optional_binary_or_null(attach_opaque_data));
    arrays.push_back(optional_binary_or_null(transaction_opaque_data));
    arrays.push_back(vgi::BuildBoolScalar(resolved_secrets_provided));
    arrays.push_back(vgi::BuildOptionalStringScalar(std::nullopt));  // at_unit
    arrays.push_back(vgi::BuildOptionalStringScalar(std::nullopt));  // at_value
    arrays.push_back(null_struct(copy_from_type));
    arrays.push_back(null_struct(copy_to_type));
    arrays.push_back(vgi::BuildOptionalStringScalar(schema_name));

    return arrow::RecordBatch::Make(schema, 1, arrays);
}

BindResponseResult ParseBindResponse(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) throw std::runtime_error("empty BindResponse");
    BindResponseResult result;

    auto schema_col = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("output_schema"));
    if (!schema_col || schema_col->IsNull(0)) throw std::runtime_error("BindResponse missing output_schema");
    auto view = schema_col->GetView(0);
    auto buffer = arrow::Buffer::Wrap(view.data(), view.size());
    arrow::io::BufferReader reader(buffer);
    arrow::ipc::DictionaryMemo dict_memo;
    auto schema_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
    check_ok(schema_result.status(), "deserializing BindResponse.output_schema");
    result.output_schema = schema_result.ValueUnsafe();

    if (auto opaque_col = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("opaque_data"))) {
        if (!opaque_col->IsNull(0)) {
            auto opaque_view = opaque_col->GetView(0);
            result.opaque_data.assign(opaque_view.data(), opaque_view.data() + opaque_view.size());
        }
    }
    return result;
}

std::shared_ptr<arrow::RecordBatch> BuildInitRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& bind_opaque_data, const std::vector<int64_t>& projection_ids,
    const std::optional<std::string>& pushdown_filters, const std::vector<std::string>& join_keys,
    std::optional<int64_t> row_limit, const std::optional<std::string>& phase,
    const std::vector<std::vector<uint8_t>>& split_tokens) {
    static const std::vector<std::string> phase_values = {"INPUT", "FINALIZE", "TABLE_BUFFERING",
                                                            "TABLE_BUFFERING_FINALIZE"};
    static const std::vector<std::string> direction_values = {"ASC", "DESC"};
    static const std::vector<std::string> null_order_values = {"NULLS_FIRST", "NULLS_LAST"};
    auto phase_type = arrow::dictionary(arrow::int16(), arrow::utf8());
    auto direction_type = arrow::dictionary(arrow::int16(), arrow::utf8());
    auto null_order_type = arrow::dictionary(arrow::int16(), arrow::utf8());

    auto schema = arrow::schema({
        arrow::field("bind_call", arrow::binary(), false),
        arrow::field("output_schema", arrow::binary(), false),
        arrow::field("bind_opaque_data", arrow::binary(), true),
        arrow::field("projection_ids", arrow::list(arrow::int64()), true),
        arrow::field("pushdown_filters", arrow::large_binary(), true),
        arrow::field("join_keys", arrow::list(arrow::large_binary()), true),
        arrow::field("split_tokens", arrow::list(arrow::large_binary()), true),
        arrow::field("row_limit", arrow::int64(), true),
        arrow::field("phase", phase_type, true),
        arrow::field("finalize_state_id", arrow::binary(), true),
        arrow::field("execution_id", arrow::binary(), true),
        arrow::field("init_opaque_data", arrow::binary(), true),
        arrow::field("substream_id", arrow::binary(), true),
        arrow::field("order_by_column_name", arrow::utf8(), true),
        arrow::field("order_by_direction", direction_type, true),
        arrow::field("order_by_null_order", null_order_type, true),
        arrow::field("order_by_limit", arrow::int64(), true),
        arrow::field("tablesample_percentage", arrow::float64(), true),
        arrow::field("tablesample_seed", arrow::int64(), true),
    });

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    {
        arrow::BinaryBuilder b;
        check_ok(b.Append(bind_call_bytes.data(), static_cast<int32_t>(bind_call_bytes.size())), "append bind_call");
        arrays.push_back(finish(b, "bind_call"));
    }
    {
        arrow::BinaryBuilder b;
        check_ok(b.Append(output_schema_bytes.data(), static_cast<int32_t>(output_schema_bytes.size())),
                 "append output_schema");
        arrays.push_back(finish(b, "output_schema"));
    }
    arrays.push_back(optional_binary_or_null(bind_opaque_data));
    {
        auto values = std::make_shared<arrow::Int64Builder>();
        arrow::ListBuilder list(arrow::default_memory_pool(), values);
        if (projection_ids.empty()) {
            check_ok(list.AppendNull(), "append null projection_ids");
        } else {
            check_ok(list.Append(), "start projection_ids");
            for (auto id : projection_ids) check_ok(values->Append(id), "append projection id");
        }
        arrays.push_back(finish(list, "projection_ids"));
    }
    {
        arrow::LargeBinaryBuilder b;
        if (pushdown_filters) {
            check_ok(b.Append(pushdown_filters->data(), static_cast<int64_t>(pushdown_filters->size())),
                     "append pushdown_filters");
        } else {
            check_ok(b.AppendNull(), "append null pushdown_filters");
        }
        arrays.push_back(finish(b, "pushdown_filters"));
    }
    {
        auto values = std::make_shared<arrow::LargeBinaryBuilder>();
        arrow::ListBuilder list(arrow::default_memory_pool(), values);
        if (join_keys.empty()) {
            check_ok(list.AppendNull(), "append null join_keys");
        } else {
            check_ok(list.Append(), "start join_keys");
            for (const auto& k : join_keys) check_ok(values->Append(k), "append join_keys entry");
        }
        arrays.push_back(finish(list, "join_keys"));
    }
    {
        auto values = std::make_shared<arrow::LargeBinaryBuilder>();
        arrow::ListBuilder list(arrow::default_memory_pool(), values);
        if (split_tokens.empty()) {
            check_ok(list.AppendNull(), "append null split_tokens");
        } else {
            check_ok(list.Append(), "start split_tokens");
            for (const auto& token : split_tokens) {
                check_ok(values->Append(token.data(), static_cast<int64_t>(token.size())),
                         "append split_tokens entry");
            }
        }
        arrays.push_back(finish(list, "split_tokens"));
    }
    {
        arrow::Int64Builder b;
        if (row_limit) {
            check_ok(b.Append(*row_limit), "append row_limit");
        } else {
            check_ok(b.AppendNull(), "append null row_limit");
        }
        arrays.push_back(finish(b, "row_limit"));
    }
    arrays.push_back(vgi::BuildOptionalEnumArray(phase, phase_values));    // phase
    arrays.push_back(null_binary());                                      // finalize_state_id
    arrays.push_back(null_binary());                                      // execution_id
    arrays.push_back(null_binary());                                      // init_opaque_data
    arrays.push_back(null_binary());                                      // substream_id
    arrays.push_back(vgi::BuildOptionalStringScalar(std::nullopt));       // order_by_column_name
    arrays.push_back(null_dictionary(direction_type, direction_values));  // order_by_direction
    arrays.push_back(null_dictionary(null_order_type, null_order_values));  // order_by_null_order
    {
        arrow::Int64Builder b;
        check_ok(b.AppendNull(), "append null order_by_limit");
        arrays.push_back(finish(b, "order_by_limit"));
    }
    {
        arrow::DoubleBuilder b;
        check_ok(b.AppendNull(), "append null tablesample_percentage");
        arrays.push_back(finish(b, "tablesample_percentage"));
    }
    {
        arrow::Int64Builder b;
        check_ok(b.AppendNull(), "append null tablesample_seed");
        arrays.push_back(finish(b, "tablesample_seed"));
    }

    return arrow::RecordBatch::Make(schema, 1, arrays);
}

GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch || batch->num_rows() == 0) throw std::runtime_error("empty GlobalInitResponse");
    GlobalInitResponseResult result;
    if (auto exec = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("execution_id"))) {
        if (!exec->IsNull(0)) {
            auto view = exec->GetView(0);
            result.execution_id.assign(view.data(), view.data() + view.size());
        }
    }
    if (auto workers = std::dynamic_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("max_workers"))) {
        if (!workers->IsNull(0)) result.max_workers = workers->Value(0);
    }
    if (result.max_workers <= 0) result.max_workers = 1;
    if (auto opaque = std::dynamic_pointer_cast<arrow::BinaryArray>(batch->GetColumnByName("opaque_data"))) {
        if (!opaque->IsNull(0)) {
            auto view = opaque->GetView(0);
            result.opaque_data.assign(view.data(), view.data() + view.size());
        }
    }
    return result;
}

}  // namespace vgi_sqlite
