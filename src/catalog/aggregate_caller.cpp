// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/aggregate_caller.h"

#include <stdexcept>

#include "catalog/aggregate_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

nano::BatchPtr CallUnary(VgiConnection& connection, const std::string& method,
                                              const nano::BatchPtr& params) {
    auto response = connection.CallUnary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// The 0-field const-args struct AggregateBindRequest.arguments needs -
// no aggregate this driver calls takes a ConstParam/named argument. Same
// construction as ScalarFunctionCaller's BuildEmptyArgsBytes.
std::vector<uint8_t> BuildEmptyArgsBytes() {
    auto args_schema = nano::MakeSchema(
        {nano::Field("args", nano::StructType({}), /*nullable=*/false)});
    auto args_batch = nano::Batch::Create(args_schema);
    args_batch->SetStruct("args", {});
    args_batch->FinishRow();
    return to_bytes(wire::encode_ipc(args_batch));
}

// VGI's fixed exchange-column naming convention ("col_0", "col_1", ...)
// already established for scalar functions - see ScalarFunctionCaller's
// file comment. Types come from the first Step()'s actual argument
// scalars, not catalog metadata, for the same reason.
nano::SchemaPtr SchemaFromArgs(const std::vector<nano::ScalarPtr>& args) {
    std::vector<nano::SchemaPtr> fields;
    for (size_t i = 0; i < args.size(); ++i) {
        fields.push_back(nano::Field("col_" + std::to_string(i), args[i]->type, /*nullable=*/true));
    }
    return nano::MakeSchema(fields);
}

// {group_id: int64}, exactly 1 row - shared shape for aggregate_finalize's
// and aggregate_destructor's group_ids_batch, both always addressing this
// caller's one group (see the header's file comment).
std::vector<uint8_t> BuildGroupIdBatch(int64_t group_id) {
    auto schema = nano::MakeSchema(
        {nano::Field("group_id", nano::Type(NANOARROW_TYPE_INT64), false)});
    auto batch = nano::Batch::FromScalars(schema, {nano::Scalar::Signed(group_id)});
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

void AggregateCaller::EnsureBound(const std::vector<nano::ScalarPtr>& args) {
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

void AggregateCaller::Step(const std::vector<nano::ScalarPtr>& args) {
    if (static_cast<int>(args.size()) != num_args_) {
        throw std::runtime_error("aggregate function '" + function_name_ + "': expected " +
                                  std::to_string(num_args_) + " arguments, got " + std::to_string(args.size()));
    }
    EnsureBound(args);

    std::vector<nano::SchemaPtr> fields = {
        nano::Field("__vgi_group_id", nano::Type(NANOARROW_TYPE_INT64), false)};
    for (size_t i = 0; i < args.size(); ++i) {
        // Cast rather than require an exact type match - unlike
        // ScalarFunctionCaller (which now re-derives arg_types_ fresh on
        // every call, see its own file comment), this caller genuinely
        // can't do that: execution_id-keyed accumulation is bound to one
        // fixed input_schema for the whole group (see EnsureBound/the
        // header's file comment on why re-binding mid-group isn't an
        // option), so a later Step() with a differently-typed argument
        // than the row that happened to bind first has no choice but to
        // coerce into the locked type. The nanoarrow conversion must reject
        // narrowing numeric casts like double->int64 - exactly the mechanism
        // that produced this driver's earlier scalar-function bug
        // (`add_values(1.5, 2.5)` silently computing as 1+2=3). A genuinely
        // lossy conversion therefore fails loudly here, surfacing a clear
        // mid-aggregation type-mismatch error to the SQL caller rather
        // than a silently wrong sum/count. A caller mixing incompatible
        // types across rows of one GROUP BY group now gets an explicit
        // error instead of wrong results - a real, documented limitation
        // (unlike ScalarFunctionCaller, this class can't just re-resolve
        // per call), not a silent one.
        fields.push_back(nano::Schema::Copy(*arg_types_->field(static_cast<int>(i))));
    }
    auto input_batch = nano::Batch::Create(nano::MakeSchema(fields));
    input_batch->AppendScalar(0, *nano::Scalar::Signed(0));
    for (size_t i = 0; i < args.size(); ++i) {
        try {
            input_batch->AppendScalar(static_cast<int64_t>(i + 1), *args[i]);
        } catch (const std::exception& error) {
            throw std::runtime_error("aggregate function '" + function_name_ + "': argument " +
                                     std::to_string(i) + " (" + nano::TypeName(args[i]->type->raw()) +
                                     ") doesn't match the type bound on this caller's first call (" +
                                     nano::TypeName(*arg_types_->field(static_cast<int>(i))) +
                                     "): " + error.what());
        }
    }
    input_batch->FinishRow();
    auto input_bytes = to_bytes(wire::encode_ipc(input_batch));

    auto inner = BuildAggregateUpdateRequest(function_name_, execution_id_, input_bytes,
                                             to_bytes((*checkout_)->attach_opaque_data), schema_name_);
    auto params = gen::BuildAggregateUpdateParams(to_bytes(wire::encode_ipc(inner)));
    CallUnary((*checkout_)->connection, "aggregate_update", params);
}

nano::BatchPtr AggregateCaller::Finalize() {
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
