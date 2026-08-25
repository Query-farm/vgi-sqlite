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
    auto response = connection.CallUnary(method, params);
    return wire::get_ipc(response.batch, "result");
}

// ScanFunctionResultSchema.arguments decodes to a *flat* batch, one
// column per argument named "arg_N" (catalog_interface.py's scan-function
// wiring convention) - and, for a genuinely zero-argument function,
// *zero rows* too (confirmed against vgi-fixture-worker's TenThousandFunction:
// num_columns=0 *and* num_rows=0, not the 1-row-of-nothing that would have
// been the more obvious encoding). But BindRequest.arguments needs each
// argument's IPC bytes to decode to exactly one row - a batch with one
// column named "args" whose type is a struct with fields named
// "positional_N" (vgi-python's Arguments.encoded_dict()/decode() - decode()
// only recognizes keys starting "positional_"/"named_", and
// deserialize_from_bytes() indexes row 0 of "args" unconditionally, so a
// 0-row batch fails with "IndexError: index out of bounds" resolving it -
// found by testing a no-arg table, not documented anywhere read ahead of
// time). Repackage, rename, AND normalize to exactly one row - never trust
// flat->num_rows() for the output row count.
std::vector<uint8_t> WrapAsArgsStruct(const std::shared_ptr<arrow::RecordBatch>& flat) {
    arrow::FieldVector fields;
    for (int i = 0; i < flat->num_columns(); ++i) {
        fields.push_back(arrow::field("positional_" + std::to_string(i), flat->schema()->field(i)->type(),
                                       flat->schema()->field(i)->nullable()));
    }

    // Build the one-row struct directly via StructBuilder/child builders
    // rather than StructArray::Make(children, fields) (which infers length
    // from the children and would carry over flat's own - possibly zero -
    // row count): every path here must produce exactly one struct row,
    // regardless of how many rows the flat source batch had.
    auto struct_type = arrow::struct_(fields);
    std::unique_ptr<arrow::ArrayBuilder> builder;
    auto make_status = arrow::MakeBuilder(arrow::default_memory_pool(), struct_type, &builder);
    if (!make_status.ok()) throw std::runtime_error("building args struct builder: " + make_status.ToString());
    auto* struct_builder = static_cast<arrow::StructBuilder*>(builder.get());
    if (auto append_status = struct_builder->Append(); !append_status.ok()) {
        throw std::runtime_error("appending args struct row: " + append_status.ToString());
    }
    // Every child builder must end up exactly as long as the struct's own
    // row count (1) or Finish() produces a struct claiming 1 row over
    // children that don't agree - append a real value when flat has a row
    // to read it from, else null (observed empty-args encoding: 0
    // *columns* and 0 rows together, so this braces for the case those
    // ever diverge - 0 rows with >0 columns - rather than assuming it away).
    for (int i = 0; i < flat->num_columns(); ++i) {
        auto* field_builder = struct_builder->field_builder(i);
        if (flat->num_rows() == 0) {
            if (auto status = field_builder->AppendNull(); !status.ok()) {
                throw std::runtime_error("appending null arg " + std::to_string(i) + ": " + status.ToString());
            }
            continue;
        }
        auto scalar_result = flat->column(i)->GetScalar(0);
        if (!scalar_result.ok()) {
            throw std::runtime_error("reading arg " + std::to_string(i) + ": " + scalar_result.status().ToString());
        }
        if (auto append_status = field_builder->AppendScalar(**scalar_result); !append_status.ok()) {
            throw std::runtime_error("appending arg " + std::to_string(i) + ": " + append_status.ToString());
        }
    }
    auto finish_result = struct_builder->Finish();
    if (!finish_result.ok()) throw std::runtime_error("finishing args struct: " + finish_result.status().ToString());
    auto struct_array = finish_result.ValueUnsafe();

    auto args_schema = arrow::schema({arrow::field("args", struct_type, false)});
    auto args_batch = arrow::RecordBatch::Make(args_schema, /*num_rows=*/1, {struct_array});
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
            stream_->Close();
        } catch (...) {
            // Best-effort: a connection already broken for other reasons
            // shouldn't turn a cursor teardown into a crash.
        }
    }
}

void TableScanner::Bind(const ScanFunction& scan_function, const std::optional<std::string>& schema_name,
                        const std::optional<std::vector<uint8_t>>& transaction_opaque_data) {
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
                                   transaction_opaque_data,
                                   /*resolved_secrets_provided=*/false, schema_name);
    bind_call_bytes_ = to_bytes(wire::encode_ipc(inner));
    auto params = gen::BuildBindParams(bind_call_bytes_);
    auto result = Call(connection_, "bind", params);
    auto parsed = ParseBindResponse(result);
    bind_.output_schema = parsed.output_schema;
    bind_.opaque_data = parsed.opaque_data;
}

void TableScanner::Init(const std::vector<int64_t>& projection_ids,
                        const std::optional<std::string>& pushdown_filters,
                        std::optional<int64_t> row_limit) {
    if (!bind_.output_schema) throw std::runtime_error("TableScanner::Init called before Bind");
    auto output_schema_bytes = to_bytes(wire::encode_schema(bind_.output_schema));
    auto inner = BuildInitRequest(bind_call_bytes_, output_schema_bytes,
                                   bind_.opaque_data.empty()
                                       ? std::nullopt
                                       : std::optional<std::vector<uint8_t>>(bind_.opaque_data),
                                   projection_ids, pushdown_filters, /*join_keys=*/{}, row_limit);
    auto init_bytes = to_bytes(wire::encode_ipc(inner));
    auto params = gen::BuildInitParams(init_bytes);
    // init is the one Stream-kind method: it returns a GlobalInitResponse
    // header, then a producer stream of output batches.
    stream_ = connection_.OpenProducer("init", params, bind_.output_schema, /*has_header=*/true);
}

std::optional<std::shared_ptr<arrow::RecordBatch>> TableScanner::Next() {
    if (!stream_) throw std::runtime_error("TableScanner::Next called before Init");
    // tick() returning nullopt is the real end-of-stream signal (the
    // worker closed its response stream, i.e. called out.finish()). A
    // present-but-0-row batch is a legitimate mid-stream tick, not EOF -
    // skip empty ticks rather than surfacing them as rows, but keep
    // pulling until the stream actually ends.
    for (;;) {
        auto batch = stream_->Tick();
        if (!batch) return std::nullopt;
        if (batch->batch && batch->batch->num_rows() > 0) return batch->batch;
    }
}

}  // namespace vgi_sqlite
