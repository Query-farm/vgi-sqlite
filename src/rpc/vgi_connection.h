// © Copyright 2026 Query Farm LLC - https://query.farm
//
// VgiConnection: the SQLite driver's thin wrapper over vgi_rpc::RpcClient.
//
// It owns nothing VGI-protocol-specific (no request builders, no catalog
// model) - that lives in src/catalog/ once it exists. Its whole job is:
// pick a transport from a worker "location" string, spawn/connect it, and
// stamp every request with this driver's protocol version so a mismatched
// worker fails loudly and early (see vgi's protocol_version/version_mismatch
// test for the behavior this mirrors).
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <vgi_rpc/client.h>

namespace vgi_sqlite {

// A worker "location" as accepted by vgi_attach(). MVP supports only the
// subprocess form (a shell-less argv, matching VGI's default transport and
// DuckDB's `ATTACH ... LOCATION 'uv run worker.py'` model). unix://, tcp://,
// http(s):// are follow-up work (see the plan's Phase 2/3/4).
class VgiConnection {
public:
    // `argv` is executed directly, never through a shell (matches
    // vgi_rpc::ClientTransport::spawn's contract).
    static VgiConnection spawn(const std::vector<std::string>& argv);

    vgi_rpc::RpcClient& client() noexcept { return client_; }

    // Convenience passthrough used by the connection proof-of-concept and by
    // debugging tools: the RPC framework's own __describe__ method, which
    // requires no VGI-protocol-level request building at all.
    vgi_rpc::ServiceDescription describe();

private:
    explicit VgiConnection(vgi_rpc::RpcClient client);

    vgi_rpc::RpcClient client_;
};

}  // namespace vgi_sqlite
