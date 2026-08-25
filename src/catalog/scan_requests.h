// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Hand-coded inner request builders / response parsers for the function
// bind/init lifecycle - bind's and init's outer params schemas are both
// just {request: binary}, wrapping these inner dataclasses. Ported from
// vgi's (the DuckDB extension's) vgi_rpc_types.{hpp,cpp} for field layout,
// with no DuckDB dependency. Field order/nullability must match
// vgi-python's BindRequest/InitRequest exactly - see that ported code's
// comments on why (a worker that validates its declared parameter contract
// with Schema.Equal is name/type/order/nullability-sensitive).
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>

namespace vgi_sqlite {

// bind_request_bytes = wire::encode_ipc(BuildBindRequest(...)), passed to
// generated::BuildBindParams(bind_request_bytes) for the outer wire params.
// arguments_ipc_bytes: IPC-encoded 1-row RecordBatch, one column per
// argument (positional_N / named_<name> fields per the protocol's argument
// convention) - a zero-argument function encodes this as a 1-row batch of
// zero columns, not an empty/null value (BindRequest.arguments is
// non-nullable).
std::shared_ptr<arrow::RecordBatch> BuildBindRequest(
    const std::string& function_name, const std::vector<uint8_t>& arguments_ipc_bytes,
    const std::string& function_type,  // "SCALAR", "TABLE", "AGGREGATE"
    const std::optional<std::vector<uint8_t>>& input_schema_bytes = std::nullopt,
    const std::optional<std::vector<uint8_t>>& settings_bytes = std::nullopt,
    const std::optional<std::vector<uint8_t>>& secrets_bytes = std::nullopt,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data = std::nullopt,
    const std::optional<std::vector<uint8_t>>& transaction_opaque_data = std::nullopt,
    bool resolved_secrets_provided = false,
    const std::optional<std::string>& schema_name = std::nullopt);

struct BindResponseResult {
    std::shared_ptr<arrow::Schema> output_schema;
    std::vector<uint8_t> opaque_data;  // empty if the worker sent null
};

// Parse BindResponse from the decoded inner result batch (1 row).
BindResponseResult ParseBindResponse(const std::shared_ptr<arrow::RecordBatch>& batch);

// init_request_bytes = wire::encode_ipc(BuildInitRequest(...)), passed to
// generated::BuildInitParams(init_request_bytes). Every optional field
// left at its default serializes as an explicit null, not an absence - a
// worker that validates its parameter contract by Schema.Equal rejects a
// batch with a missing column outright.
std::shared_ptr<arrow::RecordBatch> BuildInitRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& bind_opaque_data = std::nullopt,
    const std::vector<int64_t>& projection_ids = {},
    const std::optional<std::string>& pushdown_filters = std::nullopt,
    const std::vector<std::string>& join_keys = {});

// The header batch init's producer stream returns before any data batch.
struct GlobalInitResponseResult {
    std::vector<uint8_t> execution_id;
    int64_t max_workers = 1;
    std::vector<uint8_t> opaque_data;
};

GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch>& batch);

}  // namespace vgi_sqlite
