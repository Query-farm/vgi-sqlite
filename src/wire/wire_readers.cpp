// © Copyright 2026 Query Farm LLC - https://query.farm
#include "wire/wire_readers.h"

#include <sstream>
#include <stdexcept>

namespace vgi_sqlite::wire {
namespace {

bool has_column(const nano::BatchPtr& batch, const std::string& field) {
    return batch && batch->column(field).valid();
}

void require_rows(const nano::Column& value, const std::string& field) {
    if (!value.valid()) throw std::runtime_error("no field '" + field + "'");
    if (!value.length()) throw std::runtime_error("field '" + field + "' batch has no rows");
}

std::string text_value(const nano::Column& value, int64_t row, const std::string& field) {
    if (value.IsNull(row)) throw std::runtime_error("field '" + field + "' is null but not optional");
    auto actual = value;
    if (value.view->dictionary) {
        row = value.Int(row);
        actual = value.dictionary();
    }
    auto type = actual.type_id();
    if (type != NANOARROW_TYPE_STRING && type != NANOARROW_TYPE_LARGE_STRING &&
        type != NANOARROW_TYPE_BINARY && type != NANOARROW_TYPE_LARGE_BINARY &&
        type != NANOARROW_TYPE_FIXED_SIZE_BINARY)
        throw std::runtime_error("field '" + field + "' is " + actual.TypeString() +
                                 ", expected string or binary");
    return actual.String(row);
}

}  // namespace

nano::Column column(const nano::BatchPtr& batch, const std::string& field) {
    if (!batch) throw std::runtime_error("field '" + field + "' requested from a null batch");
    auto result = batch->column(field);
    if (result.valid()) return result;
    std::ostringstream have;
    for (int64_t i = 0; i < batch->num_columns(); ++i) {
        if (i) have << ", ";
        const auto* schema = batch->raw().schema.children[i];
        have << (schema->name ? schema->name : "");
    }
    throw std::runtime_error("no field '" + field + "'; batch carries [" + have.str() + "]");
}

std::string get_string(const nano::BatchPtr& batch, const std::string& field) {
    auto value = column(batch, field);
    require_rows(value, field);
    return text_value(value, 0, field);
}

std::optional<std::string> get_optional_string(const nano::BatchPtr& batch,
                                                const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto value = column(batch, field);
    require_rows(value, field);
    return value.IsNull(0) ? std::nullopt : std::optional<std::string>(text_value(value, 0, field));
}

std::string get_binary(const nano::BatchPtr& batch, const std::string& field) {
    return get_string(batch, field);
}

std::optional<std::string> get_optional_binary(const nano::BatchPtr& batch,
                                                const std::string& field) {
    return get_optional_string(batch, field);
}

bool get_bool(const nano::BatchPtr& batch, const std::string& field) {
    auto value = column(batch, field);
    require_rows(value, field);
    if (value.IsNull(0)) throw std::runtime_error("field '" + field + "' is null but not optional");
    return value.Bool(0);
}

std::optional<bool> get_optional_bool(const nano::BatchPtr& batch, const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto value = column(batch, field);
    require_rows(value, field);
    return value.IsNull(0) ? std::nullopt : std::optional<bool>(value.Bool(0));
}

int64_t get_int64(const nano::BatchPtr& batch, const std::string& field) {
    auto value = column(batch, field);
    require_rows(value, field);
    if (value.IsNull(0)) throw std::runtime_error("field '" + field + "' is null but not optional");
    return value.Int(0);
}

std::optional<int64_t> get_optional_int64(const nano::BatchPtr& batch,
                                           const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto value = column(batch, field);
    require_rows(value, field);
    return value.IsNull(0) ? std::nullopt : std::optional<int64_t>(value.Int(0));
}

std::string get_enum(const nano::BatchPtr& batch, const std::string& field) {
    return get_string(batch, field);
}

std::optional<std::string> get_optional_enum(const nano::BatchPtr& batch,
                                              const std::string& field) {
    return get_optional_string(batch, field);
}

nano::BatchPtr decode_ipc(const std::string& bytes) { return nano::DecodeBatch(bytes); }

std::vector<std::string> get_binary_list(const nano::BatchPtr& batch,
                                          const std::string& field) {
    std::vector<std::string> result;
    if (!has_column(batch, field)) return result;
    auto list = column(batch, field);
    if (!list.length() || list.IsNull(0)) return result;
    auto values = list.child(0);
    for (int64_t i = list.Offset(0); i < list.Offset(1); ++i)
        if (!values.IsNull(i)) result.push_back(values.String(i));
    return result;
}

std::vector<int64_t> get_int64_list(const nano::BatchPtr& batch,
                                     const std::string& field) {
    std::vector<int64_t> result;
    if (!has_column(batch, field)) return result;
    auto list = column(batch, field);
    if (!list.length() || list.IsNull(0)) return result;
    auto values = list.child(0);
    for (int64_t i = list.Offset(0); i < list.Offset(1); ++i)
        if (!values.IsNull(i)) result.push_back(values.Int(i));
    return result;
}

std::vector<std::string> get_string_list(const nano::BatchPtr& batch,
                                          const std::string& field) {
    return get_binary_list(batch, field);
}

std::optional<std::string> get_string_map_value(const nano::BatchPtr& batch,
                                                 const std::string& field,
                                                 const std::string& key) {
    if (!has_column(batch, field)) return std::nullopt;
    auto map = column(batch, field);
    if (!map.length() || map.IsNull(0)) return std::nullopt;
    auto entries = map.child(0);
    auto keys = entries.child(0);
    auto values = entries.child(1);
    for (int64_t i = map.Offset(0); i < map.Offset(1); ++i)
        if (!keys.IsNull(i) && keys.String(i) == key && !values.IsNull(i)) return values.String(i);
    return std::nullopt;
}

nano::BatchPtr get_ipc(const nano::BatchPtr& batch, const std::string& field) {
    auto bytes = get_optional_binary(batch, field);
    return bytes ? decode_ipc(*bytes) : nullptr;
}

std::string encode_ipc(const nano::BatchPtr& batch) {
    if (!batch) throw std::runtime_error("cannot encode a null batch");
    return nano::EncodeBatch(*batch);
}

std::string encode_schema(const nano::SchemaPtr& schema) {
    if (!schema) throw std::runtime_error("cannot encode a null schema");
    return nano::EncodeSchema(*schema);
}

nano::SchemaPtr decode_schema(const std::string& bytes) { return nano::DecodeSchema(bytes); }

nano::SchemaPtr get_schema(const nano::BatchPtr& batch, const std::string& field) {
    auto bytes = get_optional_binary(batch, field);
    return bytes ? decode_schema(*bytes) : nullptr;
}

}  // namespace vgi_sqlite::wire
