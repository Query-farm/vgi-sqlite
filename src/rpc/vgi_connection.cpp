// © Copyright 2026 Query Farm LLC - https://query.farm
#include "rpc/vgi_connection.h"

#include "vgi/generated/vgi_protocol_version.hpp"

namespace vgi_sqlite {

VgiConnection::VgiConnection(vgi_rpc::RpcClient client) : client_(std::move(client)) {}

VgiConnection VgiConnection::spawn(const std::vector<std::string>& argv) {
    vgi_rpc::RpcClientOptions options;
    // Stamped on every request; vgi_rpc's server enforces an exact
    // major.minor match at dispatch and fails the very first RPC on
    // mismatch, same as vgi's DuckDB extension does today.
    options.protocol_version = std::string(vgi::generated::VGI_PROTOCOL_VERSION);
    return VgiConnection(vgi_rpc::RpcClient::spawn(argv, options));
}

vgi_rpc::ServiceDescription VgiConnection::describe() { return client_.describe(); }

}  // namespace vgi_sqlite
