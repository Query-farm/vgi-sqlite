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

// The 0-field const-args struct BindRequest.arguments needs for this
// shape: every declared argument is an input_schema field (consumed
// per-row via Exchange()), not a separate bind-time value - same
// reasoning/construction as AggregateCaller's BuildEmptyArgsBytes and
// ScalarFunctionCaller's BuildEmptyArgsBytes (all three ported from the
// same pattern; not shared as one helper because each lives in its own
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

}  // namespace

TableInOutCaller::TableInOutCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                                   std::string schema_name, std::string function_name,
                                   std::shared_ptr<arrow::Schema> input_schema)
    : pool_(pool),
      location_(std::move(location)),
      catalog_name_(std::move(catalog_name)),
      schema_name_(std::move(schema_name)),
      function_name_(std::move(function_name)),
      input_schema_(std::move(input_schema)) {}

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

const std::shared_ptr<arrow::Schema>& TableInOutCaller::Bind() {
    if (bound_) return output_schema_;
    checkout_ = pool_.Acquire(location_, catalog_name_);

    auto input_schema_bytes = to_bytes(wire::encode_schema(input_schema_));
    auto bind_inner = BuildBindRequest(function_name_, BuildEmptyArgsBytes(), /*function_type=*/"TABLE",
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
    stream_ = (*checkout_)->connection.OpenExchange("init", init_params, input_schema_, output_schema_,
                                                     /*has_header=*/true);
    bound_ = true;
    return output_schema_;
}

std::shared_ptr<arrow::RecordBatch> TableInOutCaller::Exchange(
    const std::shared_ptr<arrow::RecordBatch>& input_row) {
    Bind();  // no-op past the first call
    auto response = stream_->Exchange(input_row);
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
