// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Usage: vgi-describe-worker <worker argv...>
//
// e.g. against the vgi-python example worker:
//   vgi-describe-worker uv run --project ~/Development/vgi-python \
//       python examples/calc_worker.py
//
// Spawns the worker as a subprocess, opens an RpcClient over its stdio, and
// prints the method inventory the worker reports via vgi_rpc's __describe__.
// This proves the transport layer (vgi_rpc::RpcClient::spawn, the Arrow IPC
// wire framing, the protocol-version stamp) works end-to-end against a real
// worker process before any VGI-protocol-specific request/response
// marshalling exists on this side.
#include <cstdio>
#include <exception>
#include <string>
#include <vector>

#include "rpc/vgi_connection.h"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <worker argv...>\n", argv[0]);
        return 2;
    }
    std::vector<std::string> worker_argv(argv + 1, argv + argc);

    try {
        auto conn = vgi_sqlite::VgiConnection::spawn(worker_argv);
        auto desc = conn.describe();

        std::printf("protocol_name:    %s\n", desc.protocol_name.c_str());
        std::printf("protocol_version: %s\n", desc.protocol_version.c_str());
        std::printf("request_version:  %s\n", desc.request_version.c_str());
        std::printf("server_id:        %s\n", desc.server_id.c_str());
        std::printf("methods (%zu):\n", desc.methods.size());
        for (const auto& [name, method] : desc.methods) {
            std::printf("  %-40s type=%-10s has_return=%s\n", name.c_str(), method.method_type.c_str(),
                        method.has_return ? "true" : "false");
        }
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
