// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/table_scanner.h"

#include <stdexcept>

#include "catalog/catalog_table_plan.h"
#include "catalog/scan_requests.h"
#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

std::vector<uint8_t> to_bytes(const std::string& s) { return {s.begin(), s.end()}; }

nano::BatchPtr Call(VgiConnection& connection, const std::string& method,
                                          const nano::BatchPtr& params) {
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
std::vector<uint8_t> WrapAsArgsStruct(const nano::BatchPtr& flat) {
    std::vector<nano::SchemaPtr> fields;
    std::vector<std::optional<std::pair<nano::Column, int64_t>>> values;
    for (int i = 0; i < flat->num_columns(); ++i) {
        const auto* source = flat->raw().schema.children[i];
        fields.push_back(nano::Field("positional_" + std::to_string(i), nano::Schema::Copy(*source),
                                     (source->flags & ARROW_FLAG_NULLABLE) != 0));
        values.push_back(flat->num_rows() == 0
                             ? std::optional<std::pair<nano::Column, int64_t>>{}
                             : std::make_optional(std::make_pair(flat->column(i), int64_t{0})));
    }
    auto args_schema = nano::MakeSchema(
        {nano::Field("args", nano::StructType(fields), /*nullable=*/false)});
    auto args_batch = nano::Batch::Create(args_schema);
    args_batch->SetStruct("args", values);
    args_batch->FinishRow();
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
    std::vector<uint8_t> args_bytes;
    if (scan_function.arguments_already_wrapped) {
        // See ScanFunction::arguments_already_wrapped's comment - the
        // caller already built the exact wire shape BindRequest.arguments
        // needs (e.g. named_<name> fields), so send it verbatim rather
        // than running it through WrapAsArgsStruct's positional rename.
        args_bytes = scan_function.arguments_ipc_bytes;
    } else {
        std::string flat_bytes(scan_function.arguments_ipc_bytes.begin(), scan_function.arguments_ipc_bytes.end());
        auto flat_args = wire::decode_ipc(flat_bytes);
        // ScanFunctionResultSchema.arguments decodes to a *flat* batch (one
        // column per argument, possibly zero); BindRequest.arguments needs
        // it repackaged as a single "args" struct column (see
        // WrapAsArgsStruct). If it doesn't decode at all, forward the raw
        // bytes unchanged rather than guess.
        args_bytes = flat_args ? WrapAsArgsStruct(flat_args) : scan_function.arguments_ipc_bytes;
    }
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
    supports_splits_ = scan_function.supports_splits;
}

void TableScanner::Init(const std::vector<int64_t>& projection_ids,
                        const std::optional<std::string>& pushdown_filters,
                        std::optional<int64_t> row_limit) {
    if (!bind_.output_schema) throw std::runtime_error("TableScanner::Init called before Bind");
    inited_ = true;
    init_projection_ids_ = projection_ids;
    init_pushdown_filters_ = pushdown_filters;
    init_row_limit_ = row_limit;

    if (supports_splits_) {
        // Plan the scan into splits instead of one whole-scan init - see
        // catalog_table_plan.h for the full design. Uses the SAME
        // projection/pushdown this scan already computed: splits doesn't
        // change what xBestIndex/xFilter decided to push down, only how
        // the resulting scan is redeemed.
        auto plan = PlanTableFunctionSplits(connection_, bind_call_bytes_, bind_.opaque_data, bind_.output_schema,
                                            projection_ids, pushdown_filters);
        pending_splits_ = std::move(plan.split_tokens);
        split_init_opaque_data_ = std::move(plan.init_opaque_data);
        next_split_index_ = 0;
        if (pending_splits_.empty()) {
            // Zero splits is legal (a fully-pruned scan) and means an
            // empty result, not an error - see catalog_table_plan.h.
            // Leaving stream_ null here is what tells Next() there's
            // nothing to tick.
            stream_.reset();
            return;
        }
        OpenNextSplit();
        return;
    }

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

void TableScanner::OpenNextSplit() {
    auto output_schema_bytes = to_bytes(wire::encode_schema(bind_.output_schema));
    auto inner = BuildInitRequest(bind_call_bytes_, output_schema_bytes,
                                   split_init_opaque_data_.empty()
                                       ? std::nullopt
                                       : std::optional<std::vector<uint8_t>>(split_init_opaque_data_),
                                   init_projection_ids_, init_pushdown_filters_, /*join_keys=*/{}, init_row_limit_,
                                   /*phase=*/std::nullopt,
                                   /*split_tokens=*/{pending_splits_[next_split_index_]});
    auto init_bytes = to_bytes(wire::encode_ipc(inner));
    auto params = gen::BuildInitParams(init_bytes);
    stream_ = connection_.OpenProducer("init", params, bind_.output_schema, /*has_header=*/true);
    ++next_split_index_;
}

std::optional<nano::BatchPtr> TableScanner::Next() {
    if (!inited_) throw std::runtime_error("TableScanner::Next called before Init");
    // tick() returning nullopt is the real end-of-stream signal (the
    // worker closed its response stream, i.e. called out.finish()). A
    // present-but-0-row batch is a legitimate mid-stream tick, not EOF -
    // skip empty ticks rather than surfacing them as rows, but keep
    // pulling until the stream actually ends. Under splits, nullopt means
    // only *this split* ended - not necessarily the whole scan (a filter
    // can prune one split to nothing while later splits still have rows,
    // and a zero-row split must not end the scan any more than a
    // zero-row tick does) - so advance to the next pending split, if any,
    // and keep going; only report the whole scan's end once every split
    // (or the single non-split stream) is exhausted.
    for (;;) {
        if (!stream_) return std::nullopt;
        auto batch = stream_->Tick();
        if (!batch) {
            stream_->Close();
            stream_.reset();
            if (supports_splits_ && next_split_index_ < pending_splits_.size()) {
                OpenNextSplit();
                continue;
            }
            return std::nullopt;
        }
        if (batch->batch && batch->batch->num_rows() > 0) return batch->batch;
    }
}

}  // namespace vgi_sqlite
