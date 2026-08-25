// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/aggregate_caller.h"

#include <stdexcept>

#include <arrow/api.h>

#include "catalog/aggregate_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

std::shared_ptr<arrow::RecordBatch> CallUnary(VgiConnection& connection, const std::string& method,
                                              const std::shared_ptr<arrow::RecordBatch>& params) {
    auto response = connection.client().call_unary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// The 0-field const-args struct AggregateBindRequest.arguments needs -
// no aggregate this driver calls takes a ConstParam/named argument. Same
// construction as ScalarFunctionCaller's BuildEmptyArgsBytes.
std::vector<uint8_t> BuildEmptyArgsBytes() {
    auto struct_type = arrow::struct_({});
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto make_status = arrow::MakeBuilder(arrow::default_memory_pool(), struct_type, &builder);
    if (!make_status.ok()) throw std::runtime_error("building empty args struct: " + make_status.ToString());
    auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
    if (auto status = struct_builder->Append(); !status.ok()) {
        throw std::runtime_error("appending empty args row: " + status.ToString());
    }
    auto finish_result = struct_builder->Finish();
    if (!finish_result.ok()) throw std::runtime_error("finishing empty args struct: " + finish_result.status().ToString());
    auto args_schema = arrow::schema({arrow::field("args", struct_type, false)});
    auto args_batch = arrow::RecordBatch::Make(args_schema, 1, {finish_result.ValueUnsafe()});
    return to_bytes(wire::encode_ipc(args_batch));
}

// VGI's fixed exchange-column naming convention ("col_0", "col_1", ...)
// already established for scalar functions - see ScalarFunctionCaller's
// file comment. Types come from the first Step()'s actual argument
// scalars, not catalog metadata, for the same reason.
std::shared_ptr<arrow::Schema> SchemaFromArgs(const std::vector<std::shared_ptr<arrow::Scalar>>& args) {
    arrow::FieldVector fields;
    for (size_t i = 0; i < args.size(); ++i) {
        fields.push_back(arrow::field("col_" + std::to_string(i), args[i]->type, /*nullable=*/true));
    }
    return arrow::schema(fields);
}

// {group_id: int64}, exactly 1 row - shared shape for aggregate_finalize's
// and aggregate_destructor's group_ids_batch, both always addressing this
// caller's one group (see the header's file comment).
std::vector<uint8_t> BuildGroupIdBatch(int64_t group_id) {
    auto schema = arrow::schema({arrow::field("group_id", arrow::int64(), false)});
    auto arr = arrow::MakeArrayFromScalar(*arrow::MakeScalar(group_id), 1).ValueOrDie();
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    return to_bytes(wire::encode_ipc(batch));
}

}  // namespace

AggregateCaller::AggregateCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                                 std::string function_name, int num_args, std::optional<std::string> schema_name)
    : pool_(pool),
      location_(std::move(location)),
      catalog_name_(std::move(catalog_name)),
      function_name_(std::move(function_name)),
      num_args_(num_args),
      schema_name_(std::move(schema_name)) {}

AggregateCaller::~AggregateCaller() {
    // Best-effort cleanup, mirroring TableScanner's stream-close
    // destructor - runs even if Finalize() was never reached (a SQLite
    // error can abandon the aggregate context mid-accumulation).
    if (bound_ && checkout_) {
        try {
            auto group_ids_bytes = BuildGroupIdBatch(0);
            auto inner = BuildAggregateDestructorRequest(function_name_, execution_id_, group_ids_bytes,
                                                          to_bytes((*checkout_)->attach_opaque_data), schema_name_);
            auto params = gen::BuildAggregateDestructorParams(to_bytes(wire::encode_ipc(inner)));
            CallUnary((*checkout_)->connection, "aggregate_destructor", params);
        } catch (...) {
            // Best-effort, per the protocol's own contract ("must not
            // raise") - a connection already broken for other reasons
            // shouldn't turn context teardown into a crash.
        }
    }
}

void AggregateCaller::EnsureBound(const std::vector<std::shared_ptr<arrow::Scalar>>& args) {
    if (bound_) return;
    arg_types_ = SchemaFromArgs(args);
    checkout_ = pool_.Acquire(location_, catalog_name_);

    std::optional<std::vector<uint8_t>> input_schema_bytes;
    if (!args.empty()) input_schema_bytes = to_bytes(wire::encode_schema(arg_types_));

    auto inner = BuildAggregateBindRequest(function_name_, BuildEmptyArgsBytes(), input_schema_bytes,
                                           to_bytes((*checkout_)->attach_opaque_data), schema_name_);
    auto params = gen::BuildAggregateBindParams(to_bytes(wire::encode_ipc(inner)));
    auto result = CallUnary((*checkout_)->connection, "aggregate_bind", params);
    auto parsed = ParseAggregateBindResponse(result);
    output_schema_ = parsed.output_schema;
    execution_id_ = parsed.execution_id;
    if (!output_schema_ || output_schema_->num_fields() != 1) {
        throw std::runtime_error("aggregate function '" + function_name_ +
                                  "': bind() returned an unexpected output schema");
    }
    bound_ = true;
}

void AggregateCaller::Step(const std::vector<std::shared_ptr<arrow::Scalar>>& args) {
    if (static_cast<int>(args.size()) != num_args_) {
        throw std::runtime_error("aggregate function '" + function_name_ + "': expected " +
                                  std::to_string(num_args_) + " arguments, got " + std::to_string(args.size()));
    }
    EnsureBound(args);

    arrow::FieldVector fields = {arrow::field("__vgi_group_id", arrow::int64(), false)};
    std::vector<std::shared_ptr<arrow::Array>> columns = {
        arrow::MakeArrayFromScalar(*arrow::MakeScalar(int64_t{0}), 1).ValueOrDie()};
    for (size_t i = 0; i < args.size(); ++i) {
        // Cast rather than require an exact type match - same reasoning
        // as ScalarFunctionCaller: the schema was locked in from
        // whichever Step() happened to bind first.
        auto cast_result = args[i]->CastTo(arg_types_->field(static_cast<int>(i))->type());
        if (!cast_result.ok()) {
            throw std::runtime_error("aggregate function '" + function_name_ + "': argument " +
                                      std::to_string(i) + " (" + args[i]->type->ToString() +
                                      ") doesn't match the type bound on this caller's first call (" +
                                      arg_types_->field(static_cast<int>(i))->type()->ToString() +
                                      "): " + cast_result.status().ToString());
        }
        auto array_result = arrow::MakeArrayFromScalar(*cast_result.ValueUnsafe(), 1);
        if (!array_result.ok()) {
            throw std::runtime_error("aggregate function '" + function_name_ + "': argument " +
                                      std::to_string(i) + ": " + array_result.status().ToString());
        }
        fields.push_back(arg_types_->field(static_cast<int>(i)));
        columns.push_back(array_result.ValueUnsafe());
    }
    auto input_batch = arrow::RecordBatch::Make(arrow::schema(fields), 1, columns);
    auto input_bytes = to_bytes(wire::encode_ipc(input_batch));

    auto inner = BuildAggregateUpdateRequest(function_name_, execution_id_, input_bytes,
                                             to_bytes((*checkout_)->attach_opaque_data), schema_name_);
    auto params = gen::BuildAggregateUpdateParams(to_bytes(wire::encode_ipc(inner)));
    CallUnary((*checkout_)->connection, "aggregate_update", params);
}

std::shared_ptr<arrow::RecordBatch> AggregateCaller::Finalize() {
    if (!bound_) throw std::runtime_error("aggregate function '" + function_name_ + "': Finalize() called with no rows accumulated");
    if (finalized_) throw std::runtime_error("aggregate function '" + function_name_ + "': Finalize() called twice");
    finalized_ = true;

    auto group_ids_bytes = BuildGroupIdBatch(0);
    auto output_schema_bytes = to_bytes(wire::encode_schema(output_schema_));
    auto inner = BuildAggregateFinalizeRequest(function_name_, execution_id_, group_ids_bytes, output_schema_bytes,
                                               to_bytes((*checkout_)->attach_opaque_data), schema_name_);
    auto params = gen::BuildAggregateFinalizeParams(to_bytes(wire::encode_ipc(inner)));
    auto result = CallUnary((*checkout_)->connection, "aggregate_finalize", params);
    auto result_batch = ParseAggregateFinalizeResponse(result);
    if (!result_batch || result_batch->num_rows() == 0) {
        throw std::runtime_error("aggregate function '" + function_name_ + "': finalize() returned no rows");
    }
    return result_batch;
}

}  // namespace vgi_sqlite
