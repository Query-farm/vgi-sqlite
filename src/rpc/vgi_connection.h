// © Copyright 2026 Query Farm LLC - https://query.farm
//
// VgiConnection: the SQLite driver's thin wrapper over vgi_rpc, unifying
// its two structurally distinct client types - vgi_rpc::RpcClient
// (subprocess/unix/tcp transport) and vgi_rpc::HttpClient (HTTP, the only
// transport with auth/TLS) - behind one call_unary/open_producer/
// open_exchange-shaped surface, so every catalog/ call site (VgiCatalogClient,
// TableScanner, TableWriter, ScalarFunctionCaller, AggregateCaller) can stay
// transport-agnostic. Exactly one of the two is engaged per instance,
// chosen once at construction by Connect() and never changed.
//
// Why a manual pair of optionals rather than a std::variant or a small
// abstract base class wrapping both: RpcClient and HttpClient aren't a
// common interface with two implementations - their actual method
// signatures differ in real, non-cosmetic ways (HttpClient's open_producer/
// open_stream_exchange require explicit input/output arrow::Schema
// arguments RpcClient's raw open_producer/open_exchange don't take at all;
// HttpClient's call() wants an AnnotatedBatch request where RpcClient's
// call_unary() wants a bare RecordBatch). Papering over that at the
// RpcClient/HttpClient boundary itself would mean forking or wrapping
// vgi-rpc-c++'s own types; unifying one level up, at the shape *this
// driver's* call sites actually need (every one of them already has both
// schemas on hand from its own prior bind() call by the time it opens a
// stream - confirmed by reading each call site), is far smaller.
//
// Transport selection is driven entirely by the worker "location" string
// (the same one vgi_attach()'s caller supplies and that flows unchanged
// into every CREATE VIRTUAL TABLE this driver generates, and into every
// ConnectionPool key): http:// or https:// selects HTTP; anything else is
// split on whitespace into a subprocess argv (MVP default, matching VGI's
// own default and DuckDB's `ATTACH ... LOCATION 'uv run worker.py'` model).
// A bearer token, when the deployment needs one, is embedded as URL
// userinfo - `http://TOKEN@host:port/path` (the same convention an
// authenticated git remote uses) - rather than added as a separate
// vgi_attach() parameter, specifically because `location` is already the
// one string every one of those call sites threads through unchanged;
// adding a parallel token channel would mean plumbing it through all of
// them again. `?vgi_prefix=<value>` (a trailing query param, e.g.
// `https://host?vgi_prefix=`) overrides vgi_rpc::HttpClientConfig's own
// default RPC mount point ("/vgi") - needed for a real deployed worker
// whose RPC surface is mounted somewhere else (a Cloudflare Worker
// mounting at the bare root, `/catalog_attach` not `/vgi/catalog_attach`,
// is what this was built against); an empty value explicitly asks for
// bare method paths, distinct from omitting the parameter (which leaves
// vgi_rpc's own default untouched) - see ParseLocation's own comment for
// why this can't just be a path segment in `location` itself
// (`HttpClient::builder()` refuses a base_url with a path at all).
// `unix:///path/to.sock` connects to an already-running worker
// (RpcClient::connect_unix) instead of spawning a new subprocess per
// connection - see Connect()'s own comment for why this is deliberately
// smaller than the full launcher-protocol discovery contract
// (docs/launcher-protocol.md's AF_UNIX auto-spawn-on-demand, still a
// documented gap - see the plan's "Later phases" section). tcp://
// (RpcClient::connect_tcp) is not implemented - no bearer-auth or TLS need
// drives it the way HTTP's does, and it was never in this driver's scope.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <vgi_rpc/annotated_batch.h>
#include <vgi_rpc/client.h>
#include <vgi_rpc/http_client.h>

#include "rpc/vgi_stream.h"

namespace vgi_sqlite {

class VgiConnection {
public:
    // Parses `location` and dispatches to the subprocess or HTTP
    // transport - see the file comment for the exact syntax. This is the
    // one place location strings are interpreted; every caller
    // (ConnectionPool::Acquire, vgi_attach()) goes through this rather
    // than deciding transport kind itself.
    static VgiConnection Connect(const std::string& location);

    // `argv` is executed directly, never through a shell (matches
    // vgi_rpc::ClientTransport::spawn's contract). Exposed directly (not
    // just via Connect()) for the probe tools, which already have a
    // pre-split argv and no location string to parse.
    static VgiConnection spawn(const std::vector<std::string>& argv);

    // The RPC framework's own __describe__ method, which requires no
    // VGI-protocol-level request building at all - used by the connection
    // proof-of-concept and debugging tools.
    vgi_rpc::ServiceDescription describe();

    // Every non-void RPC's unary form. Returns the response as-is
    // (vgi_rpc::AnnotatedBatch - identical type on both transports, no
    // conversion needed on the way out); callers unwrap the standard
    // {result: binary} envelope themselves (see catalog_client.cpp's
    // Call()).
    vgi_rpc::AnnotatedBatch CallUnary(const std::string& method,
                                      const std::shared_ptr<arrow::RecordBatch>& params);

    // output_schema is mandatory for the HTTP transport (HttpClient::
    // open_producer's own required parameter) and unused by the
    // subprocess transport (raw open_producer takes no schema at all) -
    // required unconditionally here anyway rather than making it
    // transport-conditional, since every call site already has it on hand
    // from its own prior bind() call (see TableScanner::Init).
    std::unique_ptr<VgiStream> OpenProducer(const std::string& method,
                                            const std::shared_ptr<arrow::RecordBatch>& params,
                                            const std::shared_ptr<arrow::Schema>& output_schema,
                                            bool has_header = false);

    // Same reasoning as OpenProducer for output_schema; input_schema is
    // HTTP's open_stream_exchange's other mandatory schema parameter
    // (again, every call site already has it - see TableWriter::Write and
    // ScalarFunctionCaller::Call).
    std::unique_ptr<VgiStream> OpenExchange(const std::string& method,
                                            const std::shared_ptr<arrow::RecordBatch>& params,
                                            const std::shared_ptr<arrow::Schema>& input_schema,
                                            const std::shared_ptr<arrow::Schema>& output_schema,
                                            bool has_header = false);

private:
    explicit VgiConnection(vgi_rpc::RpcClient client);
    explicit VgiConnection(vgi_rpc::HttpClient client);

    // Exactly one engaged, per the file comment.
    std::optional<vgi_rpc::RpcClient> raw_client_;
    std::optional<vgi_rpc::HttpClient> http_client_;
};

}  // namespace vgi_sqlite
