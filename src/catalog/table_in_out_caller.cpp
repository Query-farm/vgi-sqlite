// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/table_in_out_caller.h"

#include <stdexcept>
#include <utility>

#include "catalog/scan_requests.h"
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

// The 0-field const-args struct BindRequest.arguments needs when there are
// no named arguments to send (or for Bind()'s throwaway schema-only
// probe) - same construction as AggregateCaller/ScalarFunctionCaller's own
// BuildEmptyArgsBytes (not shared as one helper - each lives in its own
// translation unit and the construction is a handful of lines).
std::vector<uint8_t> BuildEmptyArgsBytes() {
    auto args_schema = nano::MakeSchema(
        {nano::Field("args", nano::StructType({}), /*nullable=*/false)});
    auto args_batch = nano::Batch::Create(args_schema);
    args_batch->SetStruct("args", {});
    args_batch->FinishRow();
    return to_bytes(wire::encode_ipc(args_batch));
}

// {args: struct<named_<name0>, named_<name1>, ...>}, one row, built from
// `row`'s columns at `indices` - the "named_<name>" wire convention
// vgi-python's Arguments.decode() recognizes (see
// TableScanner::WrapAsArgsStruct's own comment on the sibling
// "positional_N" convention; decode() accepts both prefixes on the same
// struct). Always exactly one row, matching BindRequest.arguments'
// contract regardless of `row`'s own row count (mirrors WrapAsArgsStruct's
// same normalization, for the same reason: the source must never dictate
// this batch's row count).
std::vector<uint8_t> BuildNamedArgsStruct(const nano::BatchPtr& row,
                                          const std::vector<int>& indices) {
    if (indices.empty()) return BuildEmptyArgsBytes();
    std::vector<nano::SchemaPtr> fields;
    std::vector<std::optional<std::pair<nano::Column, int64_t>>> values;
    fields.reserve(indices.size());
    for (int idx : indices) {
        const auto* field = row->raw().schema.children[idx];
        fields.push_back(nano::Field("named_" + std::string(field->name ? field->name : ""),
                                     nano::Schema::Copy(*field),
                                     (field->flags & ARROW_FLAG_NULLABLE) != 0));
        values.push_back(std::make_optional(std::make_pair(row->column(idx), int64_t{0})));
    }
    auto args_schema = nano::MakeSchema(
        {nano::Field("args", nano::StructType(fields), /*nullable=*/false)});
    auto args_batch = nano::Batch::Create(args_schema);
    args_batch->SetStruct("args", values);
    args_batch->FinishRow();
    return to_bytes(wire::encode_ipc(args_batch));
}

// True iff `field` carries VGI's "vgi_arg: named" metadata (a DuckDB
// `:=`-style keyword argument) - see this file's header comment.
bool IsNamedArgument(const ArrowSchema& field) {
    return nano::MetadataValue(field, "vgi_arg") == "named";
}

// A subset RecordBatch over `row` at `indices`, in that order - used to
// build the stream-only (positional-only) row Exchange() actually sends.
nano::BatchPtr ProjectColumns(const nano::BatchPtr& row,
                                                    const nano::SchemaPtr& projected_schema,
                                                    const std::vector<int>& indices) {
    auto projected = nano::Batch::Create(projected_schema);
    for (int64_t input_row = 0; input_row < row->num_rows(); ++input_row) {
        for (size_t output_column = 0; output_column < indices.size(); ++output_column)
            projected->AppendColumnValue(static_cast<int64_t>(output_column),
                                         row->column(indices[output_column]), input_row);
        projected->FinishRow();
    }
    return projected;
}

}  // namespace

TableInOutCaller::TableInOutCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                                   std::string schema_name, std::string function_name,
                                   nano::SchemaPtr input_schema)
    : pool_(pool),
      location_(std::move(location)),
      catalog_name_(std::move(catalog_name)),
      schema_name_(std::move(schema_name)),
      function_name_(std::move(function_name)),
      input_schema_(std::move(input_schema)) {
    std::vector<nano::SchemaPtr> stream_fields;
    for (int i = 0; i < input_schema_->num_fields(); ++i) {
        const auto* field = input_schema_->field(i);
        if (IsNamedArgument(*field)) {
            named_field_indices_.push_back(i);
        } else {
            stream_field_indices_.push_back(i);
            stream_fields.push_back(nano::Schema::Copy(*field));
        }
    }
    stream_schema_ = nano::MakeSchema(stream_fields);
}

TableInOutCaller::~TableInOutCaller() {
    // Best-effort, mirroring TableScanner's own destructor: an unclosed
    // stream "aborts its connection instead of draining" per
    // vgi_rpc::ClientStream's contract (see CLAUDE.md) - explicit Close()
    // is required even for a stream this caller never sent EOS to itself
    // (this driver never calls CloseInputWriter/sends a final empty
    // input - it just stops calling Exchange() and drops the stream,
    // matching the "one connection per cursor, closed at xClose" design;
    // see the header's file comment). checkout_'s own destructor then
    // returns the connection to the pool.
    if (stream_) {
        try {
            stream_->Close();
        } catch (...) {
            // Best-effort - a connection already broken shouldn't turn
            // cursor teardown into a crash.
        }
    }
}

const nano::SchemaPtr& TableInOutCaller::EnsureBoundWithArgs(std::vector<uint8_t> args_bytes) {
    if (bound_) return output_schema_;
    checkout_ = pool_.Acquire(location_, catalog_name_);

    auto input_schema_bytes = to_bytes(wire::encode_schema(stream_schema_));
    auto bind_inner = BuildBindRequest(function_name_, args_bytes, /*function_type=*/"TABLE",
                                       input_schema_bytes, /*settings_bytes=*/std::nullopt,
                                       /*secrets_bytes=*/std::nullopt, to_bytes((*checkout_)->attach_opaque_data),
                                       /*transaction_opaque_data=*/std::nullopt,
                                       /*resolved_secrets_provided=*/false, schema_name_);
    auto bind_call_bytes = to_bytes(wire::encode_ipc(bind_inner));
    auto bind_params = gen::BuildBindParams(bind_call_bytes);
    auto bind_result = CallUnary((*checkout_)->connection, "bind", bind_params);
    auto parsed = ParseBindResponse(bind_result);
    if (!parsed.output_schema) {
        throw std::runtime_error("table_in_out function '" + function_name_ +
                                  "': bind() returned no output schema");
    }
    output_schema_ = parsed.output_schema;

    // init(phase="INPUT") - no projection_ids, no pushdown_filters: see
    // the header's file comment on why neither is sent for this function
    // shape. Opens the exchange stream immediately; the worker doesn't
    // flush its own output schema/first response until this caller sends
    // the first real input row via Exchange() below (confirmed against
    // the DuckDB client's own equivalent code path during this feature's
    // research pass - "do not attempt to read a response/schema before
    // you've written at least one input batch").
    auto init_inner = BuildInitRequest(bind_call_bytes, to_bytes(wire::encode_schema(output_schema_)),
                                       parsed.opaque_data.empty()
                                           ? std::nullopt
                                           : std::optional<std::vector<uint8_t>>(parsed.opaque_data),
                                       /*projection_ids=*/{}, /*pushdown_filters=*/std::nullopt,
                                       /*join_keys=*/{}, /*row_limit=*/std::nullopt, /*phase=*/"INPUT");
    auto init_bytes = to_bytes(wire::encode_ipc(init_inner));
    auto init_params = gen::BuildInitParams(init_bytes);
    stream_ = (*checkout_)->connection.OpenExchange("init", init_params, stream_schema_, output_schema_,
                                                     /*has_header=*/true);
    bound_ = true;
    return output_schema_;
}

const nano::SchemaPtr& TableInOutCaller::Bind() {
    return EnsureBoundWithArgs(BuildEmptyArgsBytes());
}

nano::BatchPtr TableInOutCaller::Exchange(
    const nano::BatchPtr& input_row) {
    if (!bound_) {
        // Real bind, using THIS row's actual named-argument values - see
        // the header's file comment on why Bind() (empty args) isn't
        // reused here even if it happened to already run once for this
        // instance (the vtab never calls Bind() on a cursor's own
        // caller, only Exchange() - this is the only bind path a cursor
        // ever takes).
        EnsureBoundWithArgs(BuildNamedArgsStruct(input_row, named_field_indices_));
    }
    auto stream_row = ProjectColumns(input_row, stream_schema_, stream_field_indices_);
    auto response = stream_->Exchange(stream_row);
    if (!response || !response->batch) {
        // A protocol violation for this function shape, not a normal
        // termination - see the header's file comment ("the worker must
        // never close the stream while this caller is still feeding it
        // more input rows"). Surfacing it loudly here (rather than
        // treating it as an empty/EOF result) matches this driver's
        // established "throws rather than silently mis-binding/dropping
        // rows" ethos elsewhere (ScalarFunctionCaller, AggregateCaller).
        throw std::runtime_error("table_in_out function '" + function_name_ +
                                  "': worker closed the exchange stream mid-scan (expected exactly one "
                                  "response batch per input row)");
    }
    return response->batch;
}

}  // namespace vgi_sqlite
