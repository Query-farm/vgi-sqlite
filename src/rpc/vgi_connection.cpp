// © Copyright 2026 Query Farm LLC - https://query.farm
#include "rpc/vgi_connection.h"

#include <sstream>
#include <utility>

#include "vgi/generated/vgi_protocol_version.hpp"

namespace vgi_sqlite {
namespace {

std::vector<std::string> SplitWhitespace(const std::string& s) {
    std::vector<std::string> parts;
    std::istringstream iss(s);
    std::string part;
    while (iss >> part) parts.push_back(part);
    return parts;
}

struct ParsedLocation {
    bool is_http = false;
    std::string scheme;   // "http" or "https"; only meaningful when is_http
    std::string base_url;  // scheme://host[:port][/path], credentials stripped
    std::optional<std::string> bearer_token;
    std::vector<std::string> argv;  // subprocess mode only
};

// See vgi_connection.h's file comment for the exact syntax this
// implements and why.
ParsedLocation ParseLocation(const std::string& location) {
    ParsedLocation result;
    std::string scheme;
    if (location.rfind("http://", 0) == 0) {
        scheme = "http";
    } else if (location.rfind("https://", 0) == 0) {
        scheme = "https";
    } else {
        result.argv = SplitWhitespace(location);
        return result;
    }
    result.is_http = true;
    result.scheme = scheme;

    const size_t authority_start = scheme.size() + 3;  // strlen("://")
    const size_t slash_pos = location.find('/', authority_start);
    const size_t authority_end = (slash_pos == std::string::npos) ? location.size() : slash_pos;
    // '@' only counts as a userinfo delimiter within the authority
    // component (host[:port]) - one appearing in the path/query, if any,
    // isn't a credential separator.
    const size_t at_pos = location.find('@', authority_start);
    if (at_pos != std::string::npos && at_pos < authority_end) {
        result.bearer_token = location.substr(authority_start, at_pos - authority_start);
        result.base_url = location.substr(0, authority_start) + location.substr(at_pos + 1);
    } else {
        result.base_url = location;
    }
    return result;
}

}  // namespace

VgiConnection::VgiConnection(vgi_rpc::RpcClient client) : raw_client_(std::move(client)) {}
VgiConnection::VgiConnection(vgi_rpc::HttpClient client) : http_client_(std::move(client)) {}

VgiConnection VgiConnection::spawn(const std::vector<std::string>& argv) {
    vgi_rpc::RpcClientOptions options;
    // Stamped on every request; vgi_rpc's server enforces an exact
    // major.minor match at dispatch and fails the very first RPC on
    // mismatch, same as vgi's DuckDB extension does today.
    options.protocol_version = std::string(vgi::generated::VGI_PROTOCOL_VERSION);
    return VgiConnection(vgi_rpc::RpcClient::spawn(argv, options));
}

VgiConnection VgiConnection::Connect(const std::string& location) {
    // unix:///path/to.sock - connects to an already-running worker (e.g.
    // `vgi-fixture-worker --unix path/to.sock`) instead of spawning a new
    // subprocess per connection. Lands in the same raw_client_ slot as
    // spawn() - vgi_rpc::RpcClient::connect_unix and ::spawn are both
    // "the raw/subprocess-family transport", just choosing how the other
    // end of that raw byte stream comes to exist; every downstream
    // CallUnary/OpenProducer/OpenExchange call is identical either way.
    // Not the full launcher-protocol discovery contract (AF_UNIX
    // auto-spawn-on-demand, docs/launcher-protocol.md) - deliberately
    // smaller: this driver still expects something else to have started
    // the worker and bound the socket already, which is enough for reusing
    // one long-lived worker across many connections (e.g. this repo's own
    // sqllogictest runner - see test/sqllogictest/README.md) without
    // taking on auto-discovery/spawn semantics this driver doesn't need
    // yet. Full launcher-protocol support remains a documented gap (see
    // the plan file's "Later phases" section).
    if (location.rfind("unix://", 0) == 0) {
        vgi_rpc::RpcClientOptions options;
        options.protocol_version = std::string(vgi::generated::VGI_PROTOCOL_VERSION);
        return VgiConnection(vgi_rpc::RpcClient::connect_unix(location.substr(7), options));
    }

    auto parsed = ParseLocation(location);
    if (!parsed.is_http) return spawn(parsed.argv);

    vgi_rpc::HttpClientConfig config;
    if (parsed.bearer_token && parsed.scheme == "http") {
        // Plain HTTP with a token: the caller wrote http:// explicitly
        // (not https://), so treat that as informed consent to send the
        // token unencrypted - matches this being a local/private-channel
        // opt-in same as HttpClientConfig::allow_insecure_credentials'
        // own comment describes, and is what lets this driver's tests run
        // a bearer-token-authenticated fixture worker over plain HTTP
        // without TLS setup. A real deployment should use https:// and
        // never needs this branch.
        config.allow_insecure_credentials = true;
    }
    auto builder = vgi_rpc::HttpClient::builder(parsed.base_url);
    builder.config(config);
    // Same version-stamping rationale as spawn() above.
    builder.protocol_version(std::string(vgi::generated::VGI_PROTOCOL_VERSION));
    if (parsed.bearer_token) {
        builder.header("Authorization", "Bearer " + *parsed.bearer_token);
    }
    return VgiConnection(builder.build());
}

vgi_rpc::ServiceDescription VgiConnection::describe() {
    if (raw_client_) return raw_client_->describe();
    return http_client_->describe();
}

vgi_rpc::AnnotatedBatch VgiConnection::CallUnary(const std::string& method,
                                                 const std::shared_ptr<arrow::RecordBatch>& params) {
    if (raw_client_) return raw_client_->call_unary(method, params);
    return http_client_->call(method, vgi_rpc::AnnotatedBatch::data(params));
}

namespace {

// Wraps vgi_rpc::ClientStream (subprocess transport) as a VgiStream.
class RawVgiStream final : public VgiStream {
public:
    explicit RawVgiStream(vgi_rpc::ClientStream stream) : stream_(std::move(stream)) {}

    std::optional<vgi_rpc::AnnotatedBatch> Tick() override { return stream_.tick(); }
    std::optional<vgi_rpc::AnnotatedBatch> Exchange(const std::shared_ptr<arrow::RecordBatch>& input) override {
        return stream_.exchange(input);
    }
    void Close() override { stream_.close(); }

private:
    vgi_rpc::ClientStream stream_;
};

// Wraps vgi_rpc::HttpStreamSession (HTTP transport) as a VgiStream.
class HttpVgiStream final : public VgiStream {
public:
    explicit HttpVgiStream(vgi_rpc::HttpStreamSession session) : session_(std::move(session)) {}

    std::optional<vgi_rpc::AnnotatedBatch> Tick() override { return session_.tick(); }
    std::optional<vgi_rpc::AnnotatedBatch> Exchange(const std::shared_ptr<arrow::RecordBatch>& input) override {
        return session_.exchange(vgi_rpc::AnnotatedBatch::data(input));
    }
    void Close() override { session_.close(); }  // noexcept - see vgi_stream.h

private:
    vgi_rpc::HttpStreamSession session_;
};

}  // namespace

std::unique_ptr<VgiStream> VgiConnection::OpenProducer(const std::string& method,
                                                        const std::shared_ptr<arrow::RecordBatch>& params,
                                                        const std::shared_ptr<arrow::Schema>& output_schema,
                                                        bool has_header) {
    if (raw_client_) {
        return std::make_unique<RawVgiStream>(raw_client_->open_producer(method, params, has_header));
    }
    return std::make_unique<HttpVgiStream>(
        http_client_->open_producer(method, vgi_rpc::AnnotatedBatch::data(params), output_schema, has_header));
}

std::unique_ptr<VgiStream> VgiConnection::OpenExchange(const std::string& method,
                                                        const std::shared_ptr<arrow::RecordBatch>& params,
                                                        const std::shared_ptr<arrow::Schema>& input_schema,
                                                        const std::shared_ptr<arrow::Schema>& output_schema,
                                                        bool has_header) {
    if (raw_client_) {
        return std::make_unique<RawVgiStream>(raw_client_->open_exchange(method, params, has_header));
    }
    return std::make_unique<HttpVgiStream>(http_client_->open_stream_exchange(
        method, vgi_rpc::AnnotatedBatch::data(params), input_schema, output_schema, has_header));
}

}  // namespace vgi_sqlite
