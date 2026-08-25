// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Single-row scalar/list/map/enum Arrow-array builders for the codegen'd
// VGI request builders.
//
// This is the engine-neutral counterpart of vgi's (the DuckDB extension's)
// src/include/vgi_rpc_types.hpp Build<Type>Scalar family: same function
// names and signatures (so vgi-python's cpp_request_builders codegen can
// target either header interchangeably via its --helpers-include flag),
// ported directly from vgi/src/vgi_rpc_types.cpp with no DuckDB dependency
// - pure Arrow array-builder logic throughout, throwing std::runtime_error
// instead of duckdb::IOException.
//
// Every helper appends exactly one element to a fresh builder and returns a
// single-row Array. The non-Optional variants always emit a non-null entry
// (use them for fields the schema declares nullable=false); the Optional
// variants emit a null entry when the input is std::nullopt.
//
// Lives in `namespace vgi`, not `vgi_sqlite`, deliberately: the generated
// builders in generated/vgi_request_builders.hpp call these unqualified
// from inside `namespace vgi::generated` (the same namespace vgi-c++'s
// public schemas use), and C++ unqualified lookup only reaches *enclosing*
// namespaces, not siblings - `vgi::generated` encloses `vgi`, not
// `vgi_sqlite`. Same reason vgi's own copy lives in `duckdb::vgi`
// (enclosing `duckdb::vgi::generated`), not some `vgi_helpers` namespace of
// its own. Everything else in this repo stays in `vgi_sqlite`; qualify
// calls into this header as `vgi::Build...`.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>

namespace vgi {

std::shared_ptr<arrow::Array> BuildBinaryScalarRequired(const std::vector<uint8_t>& value);
std::shared_ptr<arrow::Array> BuildOptionalBinaryScalar(
    const std::optional<std::vector<uint8_t>>& value);

std::shared_ptr<arrow::Array> BuildStringScalar(const std::string& value);
std::shared_ptr<arrow::Array> BuildOptionalStringScalar(const std::optional<std::string>& value);

std::shared_ptr<arrow::Array> BuildBoolScalar(bool value);
std::shared_ptr<arrow::Array> BuildOptionalBoolScalar(std::optional<bool> value);

std::shared_ptr<arrow::Array> BuildInt32Scalar(int32_t value);
std::shared_ptr<arrow::Array> BuildOptionalInt32Scalar(std::optional<int32_t> value);

std::shared_ptr<arrow::Array> BuildInt64Scalar(int64_t value);
std::shared_ptr<arrow::Array> BuildOptionalInt64Scalar(std::optional<int64_t> value);

// dictionary(int16, utf8) single-value arrays, for the protocol's enum fields.
// `dictionary_values` is the full member-name list (codegen inlines it
// literally at each call site); the array carries only the one selected
// value's index.
std::shared_ptr<arrow::Array> BuildEnumArray(const std::string& value,
                                              const std::vector<std::string>& dictionary_values);
std::shared_ptr<arrow::Array> BuildOptionalEnumArray(
    const std::optional<std::string>& value, const std::vector<std::string>& dictionary_values);

// list<T> single-row builders, always non-null (no field in the protocol
// declares a nullable list<simple-scalar>).
std::shared_ptr<arrow::Array> BuildStringListScalar(const std::vector<std::string>& values);
std::shared_ptr<arrow::Array> BuildBinaryListScalar(
    const std::vector<std::vector<uint8_t>>& values);
std::shared_ptr<arrow::Array> BuildInt32ListScalar(const std::vector<int32_t>& values);
std::shared_ptr<arrow::Array> BuildInt64ListScalar(const std::vector<int64_t>& values);

// map<utf8, utf8> single-row builders.
std::shared_ptr<arrow::Array> BuildStringMapScalar(
    const std::vector<std::pair<std::string, std::string>>& entries);
std::shared_ptr<arrow::Array> BuildOptionalStringMapScalar(
    const std::optional<std::vector<std::pair<std::string, std::string>>>& entries);

}  // namespace vgi
