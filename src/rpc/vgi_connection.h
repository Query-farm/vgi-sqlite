// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <vgi_rpc/client.h>

#include "rpc/vgi_stream.h"

namespace vgi_sqlite {

struct MethodDescription {
    std::string method_type;
    bool has_return = false;
};

struct ServiceDescription {
    std::string protocol_name;
    std::string protocol_version;
    std::string request_version;
    std::string server_id;
    std::map<std::string, MethodDescription> methods;
};

class VgiConnection {
public:
    VgiConnection() = default;
    ~VgiConnection();
    VgiConnection(const VgiConnection&) = delete;
    VgiConnection& operator=(const VgiConnection&) = delete;
    VgiConnection(VgiConnection&& other) noexcept;
    VgiConnection& operator=(VgiConnection&& other) noexcept;

    static VgiConnection Connect(const std::string& location);
    static VgiConnection spawn(const std::vector<std::string>& argv);

    ServiceDescription describe();
    AnnotatedBatch CallUnary(const std::string& method, const nano::BatchPtr& params);
    std::unique_ptr<VgiStream> OpenProducer(const std::string& method,
                                            const nano::BatchPtr& params,
                                            const nano::SchemaPtr& output_schema,
                                            bool has_header = false);
    std::unique_ptr<VgiStream> OpenExchange(const std::string& method,
                                            const nano::BatchPtr& params,
                                            const nano::SchemaPtr& input_schema,
                                            const nano::SchemaPtr& output_schema,
                                            bool has_header = false);

private:
    explicit VgiConnection(VgiClient* client) : client_(client) {}
    static VgiConnection ConnectOptions(VgiClientOptions& options);
    VgiClient* client_ = nullptr;
};

}  // namespace vgi_sqlite
