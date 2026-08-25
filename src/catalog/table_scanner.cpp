// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/table_scanner.h"

#include <stdexcept>

#include <arrow/api.h>

#include "catalog/scan_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

std::shared_ptr<arrow::RecordBatch> Call(VgiConnection& connection, const std::string& method,
                                          const std::shared_ptr<arrow::RecordBatch>& params) {
    auto response = connection.client().call_unary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// ScanFunctionResultSchema.arguments decodes to a *flat* 1-row batch, one
// column per argument named "arg_N" (catalog_interface.py's scan-function
// wiring convention), but BindRequest.arguments needs each argument's IPC
// bytes to decode to a batch with exactly one column named "args" whose
// type is a struct with fields named "positional_N" (vgi-python's
// Arguments.encoded_dict()/decode() - decode() only recognizes keys
// starting "positional_" or "named_"). Repackage AND rename, not just wrap.
std::vector<uint8_t> WrapAsArgsStruct(const std::shared_ptr<arrow::RecordBatch>& flat) {
    arrow::FieldVector fields;
    std::vector<std::shared_ptr<arrow::Array>> children;
    for (int i = 0; i < flat->num_columns(); ++i) {
        fields.push_back(arrow::field("positional_" + std::to_string(i), flat->schema()->field(i)->type(),
                                       flat->schema()->field(i)->nullable()));
        children.push_back(flat->column(i));
    }

    std::shared_ptr<arrow::Array> struct_array;
    if (fields.empty()) {
        // StructArray::Make requires at least one child; a zero-argument
        // function needs a valid (non-null) length-1 struct-of-nothing per
        // row, built directly via StructBuilder::Append() instead (which,
        // with no child builders to advance, just marks one row valid).
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto status = arrow::MakeBuilder(arrow::default_memory_pool(), arrow::struct_({}), &builder);
        if (!status.ok()) throw std::runtime_error("building empty-args struct builder: " + status.ToString());
        auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
        for (int64_t i = 0; i < flat->num_rows(); ++i) {
            if (auto append_status = struct_builder->Append(); !append_status.ok()) {
                throw std::runtime_error("appending empty-args row: " + append_status.ToString());
            }
        }
        auto finish_result = struct_builder->Finish();
        if (!finish_result.ok()) throw std::runtime_error("finishing empty-args struct: " + finish_result.status().ToString());
        struct_array = finish_result.ValueUnsafe();
    } else {
        auto struct_result = arrow::StructArray::Make(children, fields);
        if (!struct_result.ok()) {
            throw std::runtime_error("wrapping arguments as a struct: " + struct_result.status().ToString());
        }
        struct_array = struct_result.ValueUnsafe();
    }

    auto args_schema = arrow::schema({arrow::field("args", arrow::struct_(fields), false)});
    auto args_batch = arrow::RecordBatch::Make(args_schema, flat->num_rows(), {struct_array});
    return to_bytes(wire::encode_ipc(args_batch));
}

}  // namespace

TableScanner::TableScanner(VgiConnection& connection, std::string attach_opaque_data)
    : connection_(connection), attach_opaque_data_(std::move(attach_opaque_data)) {}

TableScanner::~TableScanner() {
    // vgi_rpc::RpcClient is single-call-at-a-time: a live ClientStream
    // reserves the connection until explicitly closed/cancelled, and
    // "destruction never drains: abandoning a live stream aborts its
    // connection instead" (vgi_rpc/client.h) - including one that already
    // reached natural end-of-stream via Next() returning nullopt, which
    // marks the *stream* finished but doesn't itself release the
    // connection's call slot. Skipping this close broke every query after
    // the first on a pooled (shared) connection with "RPC client is
    // closed" - found by testing two SELECTs against the same table in
    // one session, not documented anywhere read ahead of time.
    if (stream_) {
        try {
            stream_->close();
        } catch (...) {
            // Best-effort: a connection already broken for other reasons
            // shouldn't turn a cursor teardown into a crash.
        }
    }
}

void TableScanner::Bind(const ScanFunction& scan_function, const std::optional<std::string>& schema_name) {
    std::string flat_bytes(scan_function.arguments_ipc_bytes.begin(), scan_function.arguments_ipc_bytes.end());
    auto flat_args = wire::decode_ipc(flat_bytes);
    // ScanFunctionResultSchema.arguments decodes to a *flat* batch (one
    // column per argument, possibly zero); BindRequest.arguments needs it
    // repackaged as a single "args" struct column (see WrapAsArgsStruct).
    // If it doesn't decode at all, forward the raw bytes unchanged rather
    // than guess.
    auto args_bytes = flat_args ? WrapAsArgsStruct(flat_args) : scan_function.arguments_ipc_bytes;
    auto inner = BuildBindRequest(scan_function.function_name, args_bytes,
                                   /*function_type=*/"TABLE",
                                   /*input_schema_bytes=*/std::nullopt, /*settings_bytes=*/std::nullopt,
                                   /*secrets_bytes=*/std::nullopt, to_bytes(attach_opaque_data_),
                                   /*transaction_opaque_data=*/std::nullopt,
                                   /*resolved_secrets_provided=*/false, schema_name);
    bind_call_bytes_ = to_bytes(wire::encode_ipc(inner));
    auto params = gen::BuildBindParams(bind_call_bytes_);
    auto result = Call(connection_, "bind", params);
    auto parsed = ParseBindResponse(result);
    bind_.output_schema = parsed.output_schema;
    bind_.opaque_data = parsed.opaque_data;
}

void TableScanner::Init(const std::vector<int64_t>& projection_ids) {
    if (!bind_.output_schema) throw std::runtime_error("TableScanner::Init called before Bind");
    auto output_schema_bytes = to_bytes(wire::encode_schema(bind_.output_schema));
    auto inner = BuildInitRequest(bind_call_bytes_, output_schema_bytes,
                                   bind_.opaque_data.empty()
                                       ? std::nullopt
                                       : std::optional<std::vector<uint8_t>>(bind_.opaque_data),
                                   projection_ids);
    auto init_bytes = to_bytes(wire::encode_ipc(inner));
    auto params = gen::BuildInitParams(init_bytes);
    // init is the one Stream-kind method: it returns a GlobalInitResponse
    // header, then a producer stream of output batches.
    stream_ = connection_.client().open_producer("init", params, /*has_header=*/true);
}

std::optional<std::shared_ptr<arrow::RecordBatch>> TableScanner::Next() {
    if (!stream_) throw std::runtime_error("TableScanner::Next called before Init");
    // tick() returning nullopt is the real end-of-stream signal (the
    // worker closed its response stream, i.e. called out.finish()). A
    // present-but-0-row batch is a legitimate mid-stream tick, not EOF -
    // skip empty ticks rather than surfacing them as rows, but keep
    // pulling until the stream actually ends.
    for (;;) {
        auto batch = stream_->tick();
        if (!batch) return std::nullopt;
        if (batch->batch && batch->batch->num_rows() > 0) return batch->batch;
    }
}

}  // namespace vgi_sqlite
