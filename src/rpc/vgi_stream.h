// © Copyright 2026 Query Farm LLC - https://query.farm
//
// VgiStream: a transport-agnostic handle to one live producer/exchange
// stream. VgiConnection::OpenProducer/OpenExchange return one of these,
// backed by either vgi_rpc::ClientStream (subprocess transport) or
// vgi_rpc::HttpStreamSession (HTTP transport) - see vgi_connection.h for
// which one a given connection uses and why the two need unifying at all.
//
// Only the subset TableScanner/TableWriter/ScalarFunctionCaller actually
// use is exposed here (tick, exchange-with-a-single-input, close) - both
// underlying types offer more (cancel, header, kind, finished) that no
// call site in this driver needs yet; add them here if/when one does,
// rather than exposing the whole surface speculatively.
#pragma once

#include <memory>
#include <optional>

#include <arrow/record_batch.h>
#include <vgi_rpc/annotated_batch.h>

namespace vgi_sqlite {

class VgiStream {
public:
    virtual ~VgiStream() = default;

    // nullopt is the real end-of-stream signal (see ClientStream::tick's
    // and HttpStreamSession::tick's own contracts) - a present-but-0-row
    // batch is a legitimate mid-stream tick, not EOF. Callers already
    // handle that distinction (see TableScanner::Next).
    virtual std::optional<vgi_rpc::AnnotatedBatch> Tick() = 0;

    // Send one input batch, receive the next output batch (or nullopt if
    // the worker closed the stream without one).
    virtual std::optional<vgi_rpc::AnnotatedBatch> Exchange(
        const std::shared_ptr<arrow::RecordBatch>& input) = 0;

    // Explicit close, required even after a stream reaches natural
    // end-of-stream - see TableScanner::~TableScanner()'s comment on why
    // skipping this breaks the connection it came from.
    virtual void Close() = 0;
};

}  // namespace vgi_sqlite
