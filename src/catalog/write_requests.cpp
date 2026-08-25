// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/write_requests.h"

#include <stdexcept>

#include <arrow/api.h>

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

}  // namespace

std::vector<uint8_t> BuildWriteOptionsBytes(bool return_chunks, const std::string& on_conflict,
                                             const std::vector<std::string>& on_conflict_columns) {
    // Schema/field order/types mirror vgi's own vgi_physical_write.cpp
    // BuildWriteOptions exactly - plain utf8 for on_conflict, not
    // dictionary-encoded (unlike the protocol's own enum fields).
    auto schema = arrow::schema({
        arrow::field("return_chunks", arrow::boolean()),
        arrow::field("on_conflict", arrow::utf8()),
        arrow::field("on_conflict_columns", arrow::list(arrow::utf8())),
    });

    arrow::BooleanBuilder return_chunks_builder;
    check_ok(return_chunks_builder.Append(return_chunks), "append return_chunks");
    auto return_chunks_arr = finish(return_chunks_builder, "return_chunks");

    arrow::StringBuilder on_conflict_builder;
    check_ok(on_conflict_builder.Append(on_conflict), "append on_conflict");
    auto on_conflict_arr = finish(on_conflict_builder, "on_conflict");

    auto values = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), values);
    check_ok(list_builder.Append(), "start on_conflict_columns");
    for (const auto& col : on_conflict_columns) check_ok(values->Append(col), "append on_conflict_column");
    auto columns_arr = finish(list_builder, "on_conflict_columns");

    auto batch = arrow::RecordBatch::Make(schema, 1, {return_chunks_arr, on_conflict_arr, columns_arr});
    auto encoded = wire::encode_ipc(batch);
    return {encoded.begin(), encoded.end()};
}

std::vector<uint8_t> BuildWriteArgsStruct(const std::vector<uint8_t>& flat_positional_args_ipc_bytes,
                                           const std::vector<uint8_t>& write_options_bytes) {
    std::string flat_str(flat_positional_args_ipc_bytes.begin(), flat_positional_args_ipc_bytes.end());
    auto flat = wire::decode_ipc(flat_str);

    arrow::FieldVector fields;
    if (flat) {
        for (int i = 0; i < flat->num_columns(); ++i) {
            fields.push_back(arrow::field("positional_" + std::to_string(i), flat->schema()->field(i)->type(),
                                           flat->schema()->field(i)->nullable()));
        }
    }
    fields.push_back(arrow::field("named_write_options", arrow::binary(), /*nullable=*/true));

    auto struct_type = arrow::struct_(fields);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    check_ok(arrow::MakeBuilder(arrow::default_memory_pool(), struct_type, &builder), "make write-args struct builder");
    auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
    check_ok(struct_builder->Append(), "appending write-args struct row");

    // Every child builder must end up exactly as long as the struct's own
    // row count (1), same reasoning as table_scanner.cpp's
    // WrapAsArgsStruct - append a real value when the flat source has a
    // row to read it from, else null.
    int field_idx = 0;
    if (flat) {
        for (int i = 0; i < flat->num_columns(); ++i, ++field_idx) {
            auto* field_builder = struct_builder->field_builder(field_idx);
            if (flat->num_rows() == 0) {
                check_ok(field_builder->AppendNull(), "appending null write-args positional field");
                continue;
            }
            auto scalar_result = flat->column(i)->GetScalar(0);
            if (!scalar_result.ok()) {
                throw std::runtime_error("reading write-args positional " + std::to_string(i) + ": " +
                                          scalar_result.status().ToString());
            }
            check_ok(field_builder->AppendScalar(**scalar_result), "appending write-args positional field");
        }
    }
    // named_write_options: always present (never null) here - every caller
    // of this function has a real write_options batch to attach.
    auto* options_builder = static_cast<arrow::BinaryBuilder*>(struct_builder->field_builder(field_idx));
    check_ok(options_builder->Append(write_options_bytes.data(), static_cast<int32_t>(write_options_bytes.size())),
             "appending named_write_options");

    auto finish_result = struct_builder->Finish();
    if (!finish_result.ok()) {
        throw std::runtime_error("finishing write-args struct: " + finish_result.status().ToString());
    }
    auto struct_array = finish_result.ValueUnsafe();

    auto args_schema = arrow::schema({arrow::field("args", struct_type, false)});
    auto args_batch = arrow::RecordBatch::Make(args_schema, /*num_rows=*/1, {struct_array});
    auto encoded = wire::encode_ipc(args_batch);
    return {encoded.begin(), encoded.end()};
}

}  // namespace vgi_sqlite
