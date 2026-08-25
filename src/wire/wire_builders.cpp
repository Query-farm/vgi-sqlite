// © Copyright 2026 Query Farm LLC - https://query.farm
#include "wire/wire_builders.h"

#include <stdexcept>

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>

namespace vgi {
namespace {

void CheckStatus(const arrow::Status& status, const char* operation) {
    if (!status.ok()) {
        throw std::runtime_error(std::string("Arrow ") + operation + " failed: " + status.ToString());
    }
}

template <typename BuilderType>
std::shared_ptr<arrow::Array> FinishArray(BuilderType& builder, const char* name) {
    auto result = builder.Finish();
    if (!result.ok()) {
        throw std::runtime_error(std::string("Failed to finish Arrow builder for ") + name + ": " +
                                  result.status().ToString());
    }
    return result.ValueUnsafe();
}

std::shared_ptr<arrow::Array> BuildNullDictionaryArray(
    const std::shared_ptr<arrow::DataType>& dict_type,
    const std::vector<std::string>& dictionary_values) {
    arrow::Int16Builder index_builder;
    CheckStatus(index_builder.AppendNull(), "append null dict index");
    auto index_arr = FinishArray(index_builder, "null_dict_index");
    arrow::StringBuilder dict_builder;
    for (const auto& v : dictionary_values) {
        CheckStatus(dict_builder.Append(v), "append dict value");
    }
    auto dict_arr = FinishArray(dict_builder, "dict_values");
    auto result = arrow::DictionaryArray::FromArrays(dict_type, index_arr, dict_arr);
    if (!result.ok()) {
        throw std::runtime_error("Failed to create null dictionary array: " + result.status().ToString());
    }
    return result.ValueUnsafe();
}

}  // namespace

std::shared_ptr<arrow::Array> BuildBinaryScalarRequired(const std::vector<uint8_t>& value) {
    arrow::BinaryBuilder builder;
    CheckStatus(builder.Append(value.data(), static_cast<int32_t>(value.size())), "append binary required");
    return FinishArray(builder, "binary");
}

std::shared_ptr<arrow::Array> BuildOptionalBinaryScalar(
    const std::optional<std::vector<uint8_t>>& value) {
    arrow::BinaryBuilder builder;
    if (!value.has_value()) {
        CheckStatus(builder.AppendNull(), "append null binary");
    } else {
        CheckStatus(builder.Append(value->data(), static_cast<int32_t>(value->size())), "append binary");
    }
    return FinishArray(builder, "optional_binary");
}

std::shared_ptr<arrow::Array> BuildStringScalar(const std::string& value) {
    arrow::StringBuilder builder;
    CheckStatus(builder.Append(value), "append string");
    return FinishArray(builder, "string");
}

std::shared_ptr<arrow::Array> BuildOptionalStringScalar(const std::optional<std::string>& value) {
    arrow::StringBuilder builder;
    if (!value.has_value()) {
        CheckStatus(builder.AppendNull(), "append null string");
    } else {
        CheckStatus(builder.Append(*value), "append string");
    }
    return FinishArray(builder, "optional_string");
}

std::shared_ptr<arrow::Array> BuildBoolScalar(bool value) {
    arrow::BooleanBuilder builder;
    CheckStatus(builder.Append(value), "append bool");
    return FinishArray(builder, "bool");
}

std::shared_ptr<arrow::Array> BuildOptionalBoolScalar(std::optional<bool> value) {
    arrow::BooleanBuilder builder;
    if (!value.has_value()) {
        CheckStatus(builder.AppendNull(), "append null bool");
    } else {
        CheckStatus(builder.Append(*value), "append bool");
    }
    return FinishArray(builder, "optional_bool");
}

std::shared_ptr<arrow::Array> BuildInt32Scalar(int32_t value) {
    arrow::Int32Builder builder;
    CheckStatus(builder.Append(value), "append int32");
    return FinishArray(builder, "int32");
}

std::shared_ptr<arrow::Array> BuildOptionalInt32Scalar(std::optional<int32_t> value) {
    arrow::Int32Builder builder;
    if (!value.has_value()) {
        CheckStatus(builder.AppendNull(), "append null int32");
    } else {
        CheckStatus(builder.Append(*value), "append int32");
    }
    return FinishArray(builder, "optional_int32");
}

std::shared_ptr<arrow::Array> BuildInt64Scalar(int64_t value) {
    arrow::Int64Builder builder;
    CheckStatus(builder.Append(value), "append int64");
    return FinishArray(builder, "int64");
}

std::shared_ptr<arrow::Array> BuildOptionalInt64Scalar(std::optional<int64_t> value) {
    arrow::Int64Builder builder;
    if (!value.has_value()) {
        CheckStatus(builder.AppendNull(), "append null int64");
    } else {
        CheckStatus(builder.Append(*value), "append int64");
    }
    return FinishArray(builder, "optional_int64");
}

std::shared_ptr<arrow::Array> BuildEnumArray(const std::string& value,
                                              const std::vector<std::string>& dictionary_values) {
    arrow::StringBuilder dict_builder;
    for (const auto& v : dictionary_values) {
        CheckStatus(dict_builder.Append(v), "append enum dict value");
    }
    auto dictionary = FinishArray(dict_builder, "enum_dict");

    int16_t index = -1;
    for (size_t i = 0; i < dictionary_values.size(); i++) {
        if (dictionary_values[i] == value) {
            index = static_cast<int16_t>(i);
            break;
        }
    }
    if (index < 0) {
        throw std::runtime_error("Enum value '" + value + "' not found in dictionary");
    }

    arrow::Int16Builder index_builder;
    CheckStatus(index_builder.Append(index), "append enum index");
    auto index_array = FinishArray(index_builder, "enum_index");

    auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
    auto result = arrow::DictionaryArray::FromArrays(dict_type, index_array, dictionary);
    if (!result.ok()) {
        throw std::runtime_error("Failed to create enum array: " + result.status().ToString());
    }
    return result.ValueUnsafe();
}

std::shared_ptr<arrow::Array> BuildOptionalEnumArray(
    const std::optional<std::string>& value, const std::vector<std::string>& dictionary_values) {
    if (!value.has_value()) {
        auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
        return BuildNullDictionaryArray(dict_type, dictionary_values);
    }
    return BuildEnumArray(*value, dictionary_values);
}

std::shared_ptr<arrow::Array> BuildStringListScalar(const std::vector<std::string>& values) {
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
    CheckStatus(list_builder.Append(), "start string list");
    for (const auto& v : values) {
        CheckStatus(value_builder->Append(v), "append string item");
    }
    return FinishArray(list_builder, "string_list");
}

std::shared_ptr<arrow::Array> BuildBinaryListScalar(
    const std::vector<std::vector<uint8_t>>& values) {
    auto value_builder = std::make_shared<arrow::BinaryBuilder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
    CheckStatus(list_builder.Append(), "start binary list");
    for (const auto& v : values) {
        CheckStatus(value_builder->Append(v.data(), static_cast<int32_t>(v.size())), "append binary item");
    }
    return FinishArray(list_builder, "binary_list");
}

std::shared_ptr<arrow::Array> BuildInt32ListScalar(const std::vector<int32_t>& values) {
    auto value_builder = std::make_shared<arrow::Int32Builder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
    CheckStatus(list_builder.Append(), "start int32 list");
    for (auto v : values) {
        CheckStatus(value_builder->Append(v), "append int32 item");
    }
    return FinishArray(list_builder, "int32_list");
}

std::shared_ptr<arrow::Array> BuildInt64ListScalar(const std::vector<int64_t>& values) {
    auto value_builder = std::make_shared<arrow::Int64Builder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
    CheckStatus(list_builder.Append(), "start int64 list");
    for (auto v : values) {
        CheckStatus(value_builder->Append(v), "append int64 item");
    }
    return FinishArray(list_builder, "int64_list");
}

std::shared_ptr<arrow::Array> BuildStringMapScalar(
    const std::vector<std::pair<std::string, std::string>>& entries) {
    auto key_builder = std::make_shared<arrow::StringBuilder>();
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
    CheckStatus(builder.Append(), "start string map");
    for (const auto& [k, v] : entries) {
        CheckStatus(key_builder->Append(k), "append map key");
        CheckStatus(value_builder->Append(v), "append map value");
    }
    return FinishArray(builder, "string_map");
}

std::shared_ptr<arrow::Array> BuildOptionalStringMapScalar(
    const std::optional<std::vector<std::pair<std::string, std::string>>>& entries) {
    auto key_builder = std::make_shared<arrow::StringBuilder>();
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
    if (!entries.has_value()) {
        CheckStatus(builder.AppendNull(), "append null map");
    } else {
        CheckStatus(builder.Append(), "start string map");
        for (const auto& [k, v] : *entries) {
            CheckStatus(key_builder->Append(k), "append map key");
            CheckStatus(value_builder->Append(v), "append map value");
        }
    }
    return FinishArray(builder, "optional_string_map");
}

}  // namespace vgi
