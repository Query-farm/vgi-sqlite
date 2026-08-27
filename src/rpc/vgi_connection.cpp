// © Copyright 2026 Query Farm LLC - https://query.farm
#include "rpc/vgi_connection.h"

#include <mutex>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "rpc/launcher.h"
#include "vgi/generated/vgi_protocol_version.hpp"

namespace vgi_sqlite {
namespace {

vgi_rpc::RpcClient ConnectUnixPath(const std::string& path) {
    vgi_rpc::RpcClientOptions options;
    options.protocol_version = std::string(vgi::generated::VGI_PROTOCOL_VERSION);
    return vgi_rpc::RpcClient::connect_unix(path, options);
}

// launch:'s own rendezvous address format differs by platform - a real
// AF_UNIX socket path on POSIX, a Windows named pipe (\\.\pipe\<name>) on
// Windows (docs/launcher-protocol.md's own "Platform: Windows" section;
// see launcher.h's file comment for why - CPython's own worker CLI has no
// socket.AF_UNIX on Windows and serves a named pipe there instead, despite
// the --unix flag's name). Distinct from ConnectUnixPath above, which is
// for an EXPLICIT unix:// location the caller wrote themselves - that one
// always means genuine AF_UNIX, on every platform, and stays connect_unix
// unconditionally (verified working on Windows too, against a worker that
// binds real AF_UNIX there, e.g. vgi-go's).
vgi_rpc::RpcClient ConnectLauncherPath(const std::string& path) {
#if defined(_WIN32)
    vgi_rpc::RpcClientOptions options;
    options.protocol_version = std::string(vgi::generated::VGI_PROTOCOL_VERSION);
    return vgi_rpc::RpcClient::connect_pipe(path, options);
#else
    return ConnectUnixPath(path);
#endif
}

// Per-process cache of launch: location -> resolved socket path, mirroring
// vgi's own vgi_launcher_cache.cpp: a launch: Connect() would otherwise
// re-run the whole flock+probe+(maybe spawn) dance on every single call,
// which for the ALREADY-running-worker case is a real, avoidable cost (a
// flock acquire plus a connect probe) paid on every ConnectionPool::Acquire().
// No override-conflict tracking here (unlike the reference) - vgi_attach()
// has no mechanism yet to pass launcher_idle_timeout/launcher_state_dir
// overrides per location, so every launch: connection for a given location
// string uses LaunchConfig's plain defaults; that mechanism can be added
// later without touching this cache's shape.
std::mutex g_launch_cache_mutex;
std::unordered_map<std::string, std::string> g_launch_cache;

std::string ResolveLaunchSocketPathCached(const std::string& location) {
    {
        std::lock_guard<std::mutex> lock(g_launch_cache_mutex);
        auto it = g_launch_cache.find(location);
        if (it != g_launch_cache.end()) return it->second;
    }
    // Launch() outside the lock - a long-running spawn for one location
    // shouldn't block resolution of every other location.
    LaunchConfig cfg;
    cfg.worker_argv = launcher::ParseLaunchArgv(location.substr(std::string("launch:").size()));
    std::string path = Launch(cfg);
    std::lock_guard<std::mutex> lock(g_launch_cache_mutex);
    // First writer wins if a racing caller resolved the same location
    // concurrently - both computed the same LaunchConfig, so either
    // path is equally valid; no need to prefer one over the other.
    return g_launch_cache.try_emplace(location, path).first->second;
}

void InvalidateLaunchSocketCache(const std::string& location) {
    std::lock_guard<std::mutex> lock(g_launch_cache_mutex);
    g_launch_cache.erase(location);
}

// Resolves a launch: location to a live worker and connects, retrying once
// on a stale cache entry - mirrors vgi's own ResolveAndConnect: the cached
// worker is typically gone because its idle timeout expired between the
// prior call and this one; invalidate and resolve again (which re-probes,
// and spawns fresh if genuinely dead) rather than failing outright on a
// merely-stale cache.
vgi_rpc::RpcClient ConnectViaLauncher(const std::string& location) {
    std::string path = ResolveLaunchSocketPathCached(location);
    try {
        return ConnectLauncherPath(path);
    } catch (const std::exception&) {
        InvalidateLaunchSocketCache(location);
        path = ResolveLaunchSocketPathCached(location);
        return ConnectLauncherPath(path);
    }
}

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
    std::string base_url;  // scheme://host[:port], credentials/query stripped
    std::optional<std::string> bearer_token;
    // Overrides vgi_rpc::HttpClientConfig's own default ("/vgi") when
    // present - see the `vgi_prefix` query-param comment below. nullopt
    // (the common case) means "use vgi_rpc's own default", not "use an
    // empty prefix" - those are different: an explicit `vgi_prefix=`
    // (empty value) is how a caller asks for bare method paths.
    std::optional<std::string> http_prefix;
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
    const size_t query_pos = location.find('?', authority_start);
    // The end of the authority component (host[:port]) - whichever of a
    // path or a query string starts first, or the whole string if
    // neither is present.
    size_t authority_end = location.size();
    if (slash_pos != std::string::npos) authority_end = slash_pos;
    if (query_pos != std::string::npos && query_pos < authority_end) authority_end = query_pos;
    // '@' only counts as a userinfo delimiter within the authority
    // component (host[:port]) - one appearing in the path/query, if any,
    // isn't a credential separator.
    const size_t at_pos = location.find('@', authority_start);
    std::string authority;
    if (at_pos != std::string::npos && at_pos < authority_end) {
        result.bearer_token = location.substr(authority_start, at_pos - authority_start);
        authority = location.substr(at_pos + 1, authority_end - at_pos - 1);
    } else {
        authority = location.substr(authority_start, authority_end - authority_start);
    }
    result.base_url = location.substr(0, authority_start) + authority;

    // `?vgi_prefix=<value>` - the only query parameter this driver
    // understands, and the only way to reach a worker whose RPC mount
    // point isn't vgi_rpc::HttpClientConfig's own default `/vgi` (found
    // against a real deployed worker - a Cloudflare Worker mounting its
    // RPC surface at the bare root, `/catalog_attach` not `/vgi/
    // catalog_attach` - `HttpClient::builder(base_url)` itself refuses a
    // base_url with a path at all ("use prefix" is its own error
    // message), so this can't just be folded into the URL path the way
    // location's own scheme://host[:port] already isn't one). An empty
    // value (`?vgi_prefix=`) is the explicit, deliberate way to ask for
    // bare method paths - distinct from omitting the parameter entirely,
    // which leaves vgi_rpc's own "/vgi" default untouched. Any path
    // component before `?` (a caller writing `https://host/somepath?...`)
    // is silently dropped rather than rejected outright - HttpClient's
    // own base_url validation catches a genuine mistake there instead.
    if (query_pos != std::string::npos) {
        std::string query = location.substr(query_pos + 1);
        constexpr std::string_view kKey = "vgi_prefix=";
        if (query.rfind(kKey, 0) == 0) {
            result.http_prefix = query.substr(kKey.size());
        }
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
    // This driver still expects something else to have started the worker
    // and bound the socket already - no discovery, no spawn-on-demand.
    if (location.rfind("unix://", 0) == 0) {
        return VgiConnection(ConnectUnixPath(location.substr(7)));
    }

    // launch:<argv...> - the full launcher-discovery protocol
    // (docs/launcher-protocol.md): brings up, or reuses, a worker
    // system-wide (across this driver, vgi's own DuckDB extension,
    // `vgi-rpc launch`, any other client) per (worker_argv, cwd,
    // VGI_RPC_*-env) tuple, coordinated via a per-hash flock in a
    // per-user state directory, then connects to it the same way
    // unix:// does above. See rpc/launcher.h for the full design.
    if (launcher::IsLaunchLocation(location)) {
        return VgiConnection(ConnectViaLauncher(location));
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
    // Only override when the caller explicitly asked to (see the
    // `?vgi_prefix=` comment in ParseLocation) - otherwise leave
    // vgi_rpc::HttpClientConfig's own default ("/vgi") alone.
    if (parsed.http_prefix) {
        builder.prefix(*parsed.http_prefix);
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
