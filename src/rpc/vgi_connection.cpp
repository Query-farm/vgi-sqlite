// © Copyright 2026 Query Farm LLC - https://query.farm
#include "rpc/vgi_connection.h"

#include <cstring>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "rpc/launcher.h"
#include "vgi/generated/protocol.h"

namespace vgi_sqlite {
namespace {

std::mutex g_launch_cache_mutex;
std::unordered_map<std::string, std::string> g_launch_cache;

std::string ResolveLaunchSocketPathCached(const std::string& location) {
    {
        std::lock_guard<std::mutex> lock(g_launch_cache_mutex);
        auto it = g_launch_cache.find(location);
        if (it != g_launch_cache.end()) return it->second;
    }
    LaunchConfig cfg;
    cfg.worker_argv = launcher::ParseLaunchArgv(location.substr(std::string("launch:").size()));
    std::string path = Launch(cfg);
    std::lock_guard<std::mutex> lock(g_launch_cache_mutex);
    return g_launch_cache.try_emplace(location, path).first->second;
}

void InvalidateLaunchSocketCache(const std::string& location) {
    std::lock_guard<std::mutex> lock(g_launch_cache_mutex);
    g_launch_cache.erase(location);
}

std::vector<std::string> SplitWhitespace(const std::string& text) {
    std::vector<std::string> parts;
    std::istringstream input(text);
    std::string part;
    while (input >> part) parts.push_back(part);
    return parts;
}

struct ParsedHttpLocation {
    bool is_http = false;
    std::string target;
    std::optional<std::string> bearer_token;
};

ParsedHttpLocation ParseHttpLocation(const std::string& location) {
    ParsedHttpLocation result;
    std::string scheme;
    if (location.rfind("http://", 0) == 0)
        scheme = "http";
    else if (location.rfind("https://", 0) == 0)
        scheme = "https";
    else
        return result;
    result.is_http = true;

    const size_t authority_start = scheme.size() + 3;
    const size_t slash_pos = location.find('/', authority_start);
    const size_t query_pos = location.find('?', authority_start);
    size_t authority_end = location.size();
    if (slash_pos != std::string::npos) authority_end = slash_pos;
    if (query_pos != std::string::npos && query_pos < authority_end) authority_end = query_pos;
    const size_t at_pos = location.find('@', authority_start);
    std::string authority;
    if (at_pos != std::string::npos && at_pos < authority_end) {
        result.bearer_token = location.substr(authority_start, at_pos - authority_start);
        authority = location.substr(at_pos + 1, authority_end - at_pos - 1);
    } else {
        authority = location.substr(authority_start, authority_end - authority_start);
    }
    result.target = location.substr(0, authority_start) + authority;

    std::string prefix = "/vgi";
    if (query_pos != std::string::npos) {
        std::string query = location.substr(query_pos + 1);
        constexpr std::string_view key = "vgi_prefix=";
        if (query.rfind(key, 0) == 0) prefix = query.substr(key.size());
    }
    if (!prefix.empty() && prefix.front() != '/') prefix.insert(prefix.begin(), '/');
    result.target += prefix;
    return result;
}

std::string ClientError(VgiClient* client, int code, const std::string& operation,
                        const ArrowError* local = nullptr) {
    const auto* remote = client ? VgiClientLastError(client) : nullptr;
    if (remote && remote->message) return operation + ": " + remote->message;
    if (local && local->message[0]) return operation + ": " + local->message;
    const char* text = std::strerror(code);
    return operation + ": " + (text ? text : "VGI client error");
}

void CheckClient(VgiClient* client, int code, const std::string& operation) {
    if (code) throw std::runtime_error(ClientError(client, code, operation));
}

VgiAnnotatedBatch Borrow(const nano::Batch& batch) {
    VgiAnnotatedBatch result{};
    result.batch = batch.raw();
    ArrowBufferInit(&result.metadata);
    return result;
}

AnnotatedBatch Adopt(VgiAnnotatedBatch* value) {
    AnnotatedBatch result;
    result.batch = nano::Batch::Adopt(&value->batch);
    if (value->metadata.data && value->metadata.size_bytes)
        result.metadata.assign(reinterpret_cast<const char*>(value->metadata.data),
                               static_cast<size_t>(value->metadata.size_bytes));
    ArrowBufferReset(&value->metadata);
    return result;
}

std::string StringAt(const nano::Column& column, int64_t row) {
    return column.valid() && !column.IsNull(row) ? column.String(row) : std::string();
}

class ClientStream final : public VgiStream {
public:
    ClientStream(VgiClient* client, std::string method) : client_(client), method_(std::move(method)) {}
    ~ClientStream() override {
        if (!closed_) VgiClientStreamClose(client_, 1);
    }

    std::optional<AnnotatedBatch> Tick() override {
        VgiAnnotatedBatch output{};
        ArrowBufferInit(&output.metadata);
        int done = 0;
        int rc = VgiClientStreamTick(client_, nullptr, &output, &done);
        if (rc) {
            VgiAnnotatedBatchReset(&output);
            CheckClient(client_, rc, method_ + " stream tick");
        }
        if (done) {
            VgiAnnotatedBatchReset(&output);
            return std::nullopt;
        }
        return Adopt(&output);
    }

    std::optional<AnnotatedBatch> Exchange(const nano::BatchPtr& input) override {
        auto borrowed = Borrow(*input);
        VgiAnnotatedBatch output{};
        ArrowBufferInit(&output.metadata);
        int done = 0;
        int rc = VgiClientStreamExchange(client_, &borrowed, &output, &done);
        if (rc) {
            VgiAnnotatedBatchReset(&output);
            CheckClient(client_, rc, method_ + " stream exchange");
        }
        if (done) {
            VgiAnnotatedBatchReset(&output);
            return std::nullopt;
        }
        return Adopt(&output);
    }

    void Close() override {
        if (closed_) return;
        CheckClient(client_, VgiClientStreamClose(client_, 0), method_ + " stream close");
        closed_ = true;
    }

private:
    VgiClient* client_;
    std::string method_;
    bool closed_ = false;
};

}  // namespace

VgiConnection::~VgiConnection() {
    if (client_) VgiClientFree(client_);
}

VgiConnection::VgiConnection(VgiConnection&& other) noexcept : client_(other.client_) {
    other.client_ = nullptr;
}

VgiConnection& VgiConnection::operator=(VgiConnection&& other) noexcept {
    if (this != &other) {
        if (client_) VgiClientFree(client_);
        client_ = other.client_;
        other.client_ = nullptr;
    }
    return *this;
}

VgiConnection VgiConnection::ConnectOptions(VgiClientOptions& options) {
    options.protocol = VGI_PROTOCOL_NAME;
    options.protocol_version = VGI_PROTOCOL_VERSION;
    VgiClient* client = nullptr;
    ArrowError error;
    ArrowErrorInit(&error);
    int rc = VgiClientConnect(&options, &client, &error);
    if (rc) throw std::runtime_error(ClientError(client, rc, "connect to VGI worker", &error));
    return VgiConnection(client);
}

VgiConnection VgiConnection::spawn(const std::vector<std::string>& argv) {
    if (argv.empty()) throw std::runtime_error("empty VGI worker command");
    std::vector<const char*> pointers;
    pointers.reserve(argv.size() + 1);
    for (const auto& value : argv) pointers.push_back(value.c_str());
    pointers.push_back(nullptr);
    VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.transport = VGI_CLIENT_STDIO;
    options.argv = pointers.data();
    return ConnectOptions(options);
}

VgiConnection VgiConnection::Connect(const std::string& location) {
    if (location.rfind("unix://", 0) == 0) {
        VgiClientOptions options;
        VgiClientOptionsInit(&options);
        options.transport = VGI_CLIENT_UNIX;
        std::string target = location.substr(7);
        options.target = target.c_str();
        return ConnectOptions(options);
    }
    if (launcher::IsLaunchLocation(location)) {
        auto connect_once = [&](const std::string& path) {
            VgiClientOptions options;
            VgiClientOptionsInit(&options);
            options.transport = VGI_CLIENT_UNIX;
            options.target = path.c_str();
            return ConnectOptions(options);
        };
        auto path = ResolveLaunchSocketPathCached(location);
        try {
            return connect_once(path);
        } catch (const std::exception&) {
            InvalidateLaunchSocketCache(location);
            return connect_once(ResolveLaunchSocketPathCached(location));
        }
    }
    auto http = ParseHttpLocation(location);
    if (!http.is_http) return spawn(SplitWhitespace(location));

    VgiClientOptions options;
    VgiClientOptionsInit(&options);
    options.transport = VGI_CLIENT_HTTP;
    options.target = http.target.c_str();
    std::string authorization;
    VgiClientHeader header{};
    if (http.bearer_token) {
        authorization = "Bearer " + *http.bearer_token;
        header = {"Authorization", authorization.c_str()};
        options.headers = &header;
        options.header_count = 1;
    }
    return ConnectOptions(options);
}

ServiceDescription VgiConnection::describe() {
    VgiAnnotatedBatch listing{}, description{};
    ArrowBufferInit(&listing.metadata);
    ArrowBufferInit(&description.metadata);
    int rc = VgiClientDescribe(client_, &listing, &description);
    if (rc) {
        VgiAnnotatedBatchReset(&listing);
        VgiAnnotatedBatchReset(&description);
        CheckClient(client_, rc, "describe VGI service");
    }
    auto listed = Adopt(&listing);
    auto detail = Adopt(&description);
    ServiceDescription result;
    result.server_id = StringAt(listed.batch->column("server_id"), 0);
    result.request_version = StringAt(listed.batch->column("request_version"), 0);
    result.protocol_name = StringAt(detail.batch->column("protocol"), 0);
    result.protocol_version = StringAt(detail.batch->column("protocol_version"), 0);

    auto methods = detail.batch->column("methods");
    if (methods.valid() && !methods.IsNull(0)) {
        auto values = methods.child(0);
        for (int64_t row = methods.Offset(0); row < methods.Offset(1); ++row) {
            auto name = StringAt(values.child(0), row);
            MethodDescription method;
            method.method_type = StringAt(values.child(1), row);
            method.has_return = !values.child(2).IsNull(row) && values.child(2).Bool(row);
            result.methods.emplace(std::move(name), std::move(method));
        }
    }
    return result;
}

AnnotatedBatch VgiConnection::CallUnary(const std::string& method, const nano::BatchPtr& params) {
    auto input = Borrow(*params);
    VgiAnnotatedBatch output{};
    ArrowBufferInit(&output.metadata);
    int rc = VgiClientUnary(client_, method.c_str(), &input, &output);
    if (rc) {
        VgiAnnotatedBatchReset(&output);
        CheckClient(client_, rc, method + " unary call");
    }
    return Adopt(&output);
}

std::unique_ptr<VgiStream> VgiConnection::OpenProducer(const std::string& method,
                                                        const nano::BatchPtr& params,
                                                        const nano::SchemaPtr&,
                                                        bool has_header) {
    auto input = Borrow(*params);
    VgiAnnotatedBatch header{};
    ArrowBufferInit(&header.metadata);
    int rc = VgiClientStreamOpen(client_, method.c_str(), &input, 0, has_header ? 1 : 0,
                                 has_header ? &header : nullptr);
    VgiAnnotatedBatchReset(&header);
    CheckClient(client_, rc, method + " producer open");
    return std::make_unique<ClientStream>(client_, method);
}

std::unique_ptr<VgiStream> VgiConnection::OpenExchange(const std::string& method,
                                                        const nano::BatchPtr& params,
                                                        const nano::SchemaPtr&,
                                                        const nano::SchemaPtr&,
                                                        bool has_header) {
    auto input = Borrow(*params);
    VgiAnnotatedBatch header{};
    ArrowBufferInit(&header.metadata);
    int rc = VgiClientStreamOpen(client_, method.c_str(), &input, 1, has_header ? 1 : 0,
                                 has_header ? &header : nullptr);
    VgiAnnotatedBatchReset(&header);
    CheckClient(client_, rc, method + " exchange open");
    return std::make_unique<ClientStream>(client_, method);
}

}  // namespace vgi_sqlite
