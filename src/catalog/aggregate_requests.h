// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Hand-coded inner request builders / response parsers for VGI's aggregate
// RPC lifecycle (bind -> update* -> finalize -> destructor) - all-unary
// calls, no init/producer/exchange stream involved (confirmed by reading
// vgi-python's vgi/client/aggregate.py, the reference client this was
// ported from for field layout). Field order/nullability must match
// vgi-python's Aggregate*Request/*Response dataclasses exactly - see
// scan_requests.h's comment on why (Schema.Equal-validating workers).
//
// Grouping: VGI's own group_id concept (a caller-allocated int64 the
// worker keys its per-group accumulator state on, prepended as the first
// column - "__vgi_group_id" - of every aggregate_update input_batch) maps
// naturally onto SQLite's own per-aggregate-context model: SQLite already
// creates one fresh context per GROUP BY group (or exactly one for a
// whole-table aggregate), so AggregateCaller (see aggregate_caller.h)
// binds one execution_id per SQLite context and always uses group_id 0
// within it - no need to replicate VGI's own group-key bookkeeping
// client-side.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace vgi_sqlite {

// aggregate_bind_request_bytes = wire::encode_ipc(BuildAggregateBindRequest(...)),
// passed to generated::BuildAggregateBindParams(...). `arguments_ipc_bytes`
// is the same empty (0-field) const-args struct scalar functions bind with
// (see scalar_function_caller.cpp's BuildEmptyArgsBytes) - no aggregate
// this driver calls takes a ConstParam/named argument.
std::shared_ptr<arrow::RecordBatch> BuildAggregateBindRequest(
    const std::string& function_name, const std::vector<uint8_t>& arguments_ipc_bytes,
    const std::optional<std::vector<uint8_t>>& input_schema_bytes = std::nullopt,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data = std::nullopt,
    const std::optional<std::string>& schema_name = std::nullopt);

struct AggregateBindResponseResult {
    std::shared_ptr<arrow::Schema> output_schema;
    std::vector<uint8_t> execution_id;
};

AggregateBindResponseResult ParseAggregateBindResponse(const std::shared_ptr<arrow::RecordBatch>& batch);

// input_batch_ipc_bytes: full IPC stream of a batch whose first column is
// "__vgi_group_id" (int64, always a single row here - AggregateCaller
// calls this once per SQLite xStep) followed by the aggregate's value
// columns in declaration order (omitted entirely for a nullary aggregate
// like a COUNT(*)-style function - group_id alone carries the row).
std::shared_ptr<arrow::RecordBatch> BuildAggregateUpdateRequest(const std::string& function_name,
                                                                 const std::vector<uint8_t>& execution_id,
                                                                 const std::vector<uint8_t>& input_batch_ipc_bytes,
                                                                 const std::optional<std::vector<uint8_t>>& attach_opaque_data = std::nullopt,
                                                                 const std::optional<std::string>& schema_name = std::nullopt);

// group_ids_batch_ipc_bytes: full IPC stream of a one-column
// {group_id: int64} batch naming which group(s) to produce results for -
// always a single row (0) here, since AggregateCaller finalizes exactly
// the one group its execution_id scopes.
std::shared_ptr<arrow::RecordBatch> BuildAggregateFinalizeRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& group_ids_batch_ipc_bytes, const std::vector<uint8_t>& output_schema_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data = std::nullopt,
    const std::optional<std::string>& schema_name = std::nullopt);

// Parses AggregateFinalizeResponse.result_batch (full IPC stream bytes)
// into the decoded batch - one row per requested group_id, in that order
// (so row 0 here for AggregateCaller's always-one-group-id call).
std::shared_ptr<arrow::RecordBatch> ParseAggregateFinalizeResponse(const std::shared_ptr<arrow::RecordBatch>& batch);

std::shared_ptr<arrow::RecordBatch> BuildAggregateDestructorRequest(
    const std::string& function_name, const std::vector<uint8_t>& execution_id,
    const std::vector<uint8_t>& group_ids_batch_ipc_bytes,
    const std::optional<std::vector<uint8_t>>& attach_opaque_data = std::nullopt,
    const std::optional<std::string>& schema_name = std::nullopt);

}  // namespace vgi_sqlite
