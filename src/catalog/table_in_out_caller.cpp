// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/table_in_out_caller.h"

#include <stdexcept>
#include <utility>

#include <arrow/api.h>

#include "catalog/scan_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

std::shared_ptr<arrow::RecordBatch> CallUnary(VgiConnection& connection, const std::string& method,
                                              const std::shared_ptr<arrow::RecordBatch>& params) {
    auto response = connection.CallUnary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// The 0-field const-args struct BindRequest.arguments needs when there are
// no named arguments to send (or for Bind()'s throwaway schema-only
// probe) - same construction as AggregateCaller/ScalarFunctionCaller's own
// BuildEmptyArgsBytes (not shared as one helper - each lives in its own
// translation unit and the construction is a handful of lines).
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

// {args: struct<named_<name0>, named_<name1>, ...>}, one row, built from
// `row`'s columns at `indices` - the "named_<name>" wire convention
// vgi-python's Arguments.decode() recognizes (see
// TableScanner::WrapAsArgsStruct's own comment on the sibling
// "positional_N" convention; decode() accepts both prefixes on the same
// struct). Always exactly one row, matching BindRequest.arguments'
// contract regardless of `row`'s own row count (mirrors WrapAsArgsStruct's
// same normalization, for the same reason: the source must never dictate
// this batch's row count).
std::vector<uint8_t> BuildNamedArgsStruct(const std::shared_ptr<arrow::RecordBatch>& row,
                                          const std::vector<int>& indices) {
    if (indices.empty()) return BuildEmptyArgsBytes();
    arrow::FieldVector fields;
    fields.reserve(indices.size());
    for (int idx : indices) {
        const auto& field = row->schema()->field(idx);
        fields.push_back(arrow::field("named_" + field->name(), field->type(), field->nullable()));
    }

    // Same construction as WrapAsArgsStruct (table_scanner.cpp) - a
    // StructBuilder with one AppendScalar() per child field, not
    // StructArray::Make(children, fields) (which would infer the struct's
    // row count from the children instead of always producing exactly 1).
    auto struct_type = arrow::struct_(fields);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto make_status = arrow::MakeBuilder(arrow::default_memory_pool(), struct_type, &builder);
    if (!make_status.ok()) throw std::runtime_error("building named args struct builder: " + make_status.ToString());
    auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
    if (auto status = struct_builder->Append(); !status.ok()) {
        throw std::runtime_error("appending named args row: " + status.ToString());
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        int idx = indices[i];
        auto scalar_result = row->column(idx)->GetScalar(0);
        if (!scalar_result.ok()) {
            throw std::runtime_error("reading named argument '" + row->schema()->field(idx)->name() +
                                      "': " + scalar_result.status().ToString());
        }
        auto* field_builder = struct_builder->field_builder(static_cast<int>(i));
        if (auto status = field_builder->AppendScalar(**scalar_result); !status.ok()) {
            throw std::runtime_error("appending named argument '" + row->schema()->field(idx)->name() +
                                      "': " + status.ToString());
        }
    }
    auto finish_result = struct_builder->Finish();
    if (!finish_result.ok()) {
        throw std::runtime_error("finishing named args struct: " + finish_result.status().ToString());
    }
    auto args_schema = arrow::schema({arrow::field("args", struct_type, false)});
    auto args_batch = arrow::RecordBatch::Make(args_schema, 1, {finish_result.ValueUnsafe()});
    return to_bytes(wire::encode_ipc(args_batch));
}

// True iff `field` carries VGI's "vgi_arg: named" metadata (a DuckDB
// `:=`-style keyword argument) - see this file's header comment.
bool IsNamedArgument(const std::shared_ptr<arrow::Field>& field) {
    auto metadata = field->metadata();
    if (!metadata) return false;
    auto value = metadata->Get("vgi_arg");
    return value.ok() && *value == "named";
}

// A subset RecordBatch over `row` at `indices`, in that order - used to
// build the stream-only (positional-only) row Exchange() actually sends.
std::shared_ptr<arrow::RecordBatch> ProjectColumns(const std::shared_ptr<arrow::RecordBatch>& row,
                                                    const std::shared_ptr<arrow::Schema>& projected_schema,
                                                    const std::vector<int>& indices) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(indices.size());
    for (int idx : indices) columns.push_back(row->column(idx));
    return arrow::RecordBatch::Make(projected_schema, row->num_rows(), std::move(columns));
}

}  // namespace

TableInOutCaller::TableInOutCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                                   std::string schema_name, std::string function_name,
                                   std::shared_ptr<arrow::Schema> input_schema)
    : pool_(pool),
      location_(std::move(location)),
      catalog_name_(std::move(catalog_name)),
      schema_name_(std::move(schema_name)),
      function_name_(std::move(function_name)),
      input_schema_(std::move(input_schema)) {
    arrow::FieldVector stream_fields;
    for (int i = 0; i < input_schema_->num_fields(); ++i) {
        if (IsNamedArgument(input_schema_->field(i))) {
            named_field_indices_.push_back(i);
        } else {
            stream_field_indices_.push_back(i);
            stream_fields.push_back(input_schema_->field(i));
        }
    }
    stream_schema_ = arrow::schema(std::move(stream_fields));
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

const std::shared_ptr<arrow::Schema>& TableInOutCaller::EnsureBoundWithArgs(std::vector<uint8_t> args_bytes) {
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

const std::shared_ptr<arrow::Schema>& TableInOutCaller::Bind() {
    return EnsureBoundWithArgs(BuildEmptyArgsBytes());
}

std::shared_ptr<arrow::RecordBatch> TableInOutCaller::Exchange(
    const std::shared_ptr<arrow::RecordBatch>& input_row) {
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
