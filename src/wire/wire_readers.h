// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "wire/nanoarrow_types.h"

namespace vgi_sqlite::wire {

nano::Column column(const nano::BatchPtr& batch, const std::string& field);
std::string get_string(const nano::BatchPtr& batch, const std::string& field);
std::optional<std::string> get_optional_string(const nano::BatchPtr& batch,
                                                const std::string& field);
std::string get_binary(const nano::BatchPtr& batch, const std::string& field);
std::optional<std::string> get_optional_binary(const nano::BatchPtr& batch,
                                                const std::string& field);
bool get_bool(const nano::BatchPtr& batch, const std::string& field);
std::optional<bool> get_optional_bool(const nano::BatchPtr& batch, const std::string& field);
int64_t get_int64(const nano::BatchPtr& batch, const std::string& field);
std::optional<int64_t> get_optional_int64(const nano::BatchPtr& batch, const std::string& field);
std::string get_enum(const nano::BatchPtr& batch, const std::string& field);
std::optional<std::string> get_optional_enum(const nano::BatchPtr& batch,
                                              const std::string& field);

nano::BatchPtr decode_ipc(const std::string& bytes);
std::vector<std::string> get_binary_list(const nano::BatchPtr& batch, const std::string& field);
std::vector<int64_t> get_int64_list(const nano::BatchPtr& batch, const std::string& field);
std::vector<std::string> get_string_list(const nano::BatchPtr& batch, const std::string& field);
std::optional<std::string> get_string_map_value(const nano::BatchPtr& batch,
                                                 const std::string& field,
                                                 const std::string& key);
nano::BatchPtr get_ipc(const nano::BatchPtr& batch, const std::string& field);
std::string encode_ipc(const nano::BatchPtr& batch);
std::string encode_schema(const nano::SchemaPtr& schema);
nano::SchemaPtr decode_schema(const std::string& bytes);
nano::SchemaPtr get_schema(const nano::BatchPtr& batch, const std::string& field);

}  // namespace vgi_sqlite::wire
