// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/write_requests.h"

#include <stdexcept>

#include "wire/wire_readers.h"

namespace vgi_sqlite {
std::vector<uint8_t> BuildWriteOptionsBytes(const std::string& result_mode, const std::string& on_conflict,
                                             const std::vector<std::string>& on_conflict_columns) {
    // Schema/field order/types mirror vgi's own vgi_physical_write.cpp
    // BuildWriteOptions exactly - plain utf8 for on_conflict, not
    // dictionary-encoded (unlike the protocol's own enum fields).
    auto schema = nano::MakeSchema({
        nano::Field("result_mode", nano::Type(NANOARROW_TYPE_STRING)),
        nano::Field("on_conflict", nano::Type(NANOARROW_TYPE_STRING)),
        nano::Field("on_conflict_columns", nano::ListType(nano::Type(NANOARROW_TYPE_STRING))),
    });
    auto batch = nano::Batch::Create(schema);
    batch->SetString("result_mode", result_mode);
    batch->SetString("on_conflict", on_conflict);
    batch->SetStringList("on_conflict_columns", on_conflict_columns);
    batch->FinishRow();
    auto encoded = wire::encode_ipc(batch);
    return {encoded.begin(), encoded.end()};
}

std::vector<uint8_t> BuildWriteArgsStruct(const std::vector<uint8_t>& flat_positional_args_ipc_bytes,
                                           const std::vector<uint8_t>& write_options_bytes) {
    std::string flat_str(flat_positional_args_ipc_bytes.begin(), flat_positional_args_ipc_bytes.end());
    auto flat = wire::decode_ipc(flat_str);

    std::vector<nano::SchemaPtr> fields;
    std::vector<std::optional<std::pair<nano::Column, int64_t>>> values;
    if (flat) {
        for (int i = 0; i < flat->num_columns(); ++i) {
            const auto* source = flat->raw().schema.children[i];
            fields.push_back(nano::Field("positional_" + std::to_string(i), nano::Schema::Copy(*source),
                                         (source->flags & ARROW_FLAG_NULLABLE) != 0));
            values.push_back(flat->num_rows() == 0
                                 ? std::optional<std::pair<nano::Column, int64_t>>{}
                                 : std::make_optional(std::make_pair(flat->column(i), int64_t{0})));
        }
    }
    fields.push_back(nano::Field("named_write_options", nano::Type(NANOARROW_TYPE_BINARY), true));
    auto options_schema = nano::MakeSchema(
        {nano::Field("named_write_options", nano::Type(NANOARROW_TYPE_BINARY), true)});
    auto options = nano::Batch::FromScalars(
        options_schema, {nano::Scalar::Binary(write_options_bytes)});
    values.push_back(std::make_optional(std::make_pair(options->column(0), int64_t{0})));

    auto args_schema = nano::MakeSchema(
        {nano::Field("args", nano::StructType(fields), /*nullable=*/false)});
    auto args_batch = nano::Batch::Create(args_schema);
    args_batch->SetStruct("args", values);
    args_batch->FinishRow();
    auto encoded = wire::encode_ipc(args_batch);
    return {encoded.begin(), encoded.end()};
}

}  // namespace vgi_sqlite
