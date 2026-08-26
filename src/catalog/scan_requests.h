// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Hand-coded inner request builders / response parsers for the function
// bind/init/plan lifecycle - bind's, init's, and table_function_plan's
// outer params schemas are all just {request: binary}, wrapping these
// inner dataclasses. Ported from vgi's (the DuckDB extension's)
// vgi_rpc_types.{hpp,cpp} for field layout, with no DuckDB dependency.
// Field order/nullability must match vgi-python's BindRequest/InitRequest/
// TableFunctionPlanRequest exactly - see that ported code's comments on
// why (a worker that validates its declared parameter contract with
// Schema.Equal is name/type/order/nullability-sensitive).
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
// row_limit: plain fetch-limit hint (InitRequest.row_limit) - a worker may
// stop producing rows after this many, or may ignore it entirely. Only
// safe to pass from a caller that has independently confirmed early
// termination can't drop rows the query still needs (see vgi_vtab.cpp's
// xBestIndex comment on why that means "no other pushed WHERE constraint
// and no ORDER BY requested of this scan" for this driver specifically -
// this function itself has no way to enforce that, it just carries the
// value through).
// phase: table-in-out functions' init phase ("INPUT" or "FINALIZE" - the
// two this driver drives; "TABLE_BUFFERING"/"TABLE_BUFFERING_FINALIZE" are
// unused here). std::nullopt for every non-table-in-out call this driver
// makes (plain table scans, scalar functions) - InitRequest.phase is
// "None for other function types" (vgi-python's own doc comment).
// split_tokens: redeems one split of a planned scan (see
// catalog_table_plan.h) - a single-element list carrying exactly the
// token bytes TableFunctionPlan's ScanSplit.token gave back, opaque and
// passed through verbatim (never parsed/validated/reordered - the worker
// owns that envelope). Empty (the default) for every non-split call this
// driver makes, which InitRequest.split_tokens encodes as null, not an
// empty list - matches every other "absent" optional field's convention
// here, and vgi's own DuckDB client only ever sends zero or one token
// itself (a bin-packing multi-token client is a distinct, unimplemented
// use case - see BuildTableFunctionPlanRequest's comment on why this
// driver's plan requests never invite the worker to produce fewer, larger
// splits than it would for a client that reads one at a time).
std::shared_ptr<arrow::RecordBatch> BuildInitRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& bind_opaque_data = std::nullopt,
    const std::vector<int64_t>& projection_ids = {},
    const std::optional<std::string>& pushdown_filters = std::nullopt,
    const std::vector<std::string>& join_keys = {}, std::optional<int64_t> row_limit = std::nullopt,
    const std::optional<std::string>& phase = std::nullopt,
    const std::vector<std::vector<uint8_t>>& split_tokens = {});

// The header batch init's producer stream returns before any data batch.
struct GlobalInitResponseResult {
    std::vector<uint8_t> execution_id;
    int64_t max_workers = 1;
    std::vector<uint8_t> opaque_data;
};

GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch>& batch);

}  // namespace vgi_sqlite
