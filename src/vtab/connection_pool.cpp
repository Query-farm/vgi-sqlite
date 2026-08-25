// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/connection_pool.h"

#include <sstream>
#include <vector>

#include "catalog/catalog_client.h"

namespace vgi_sqlite {
namespace {

std::vector<std::string> SplitWhitespace(const std::string& s) {
    std::vector<std::string> parts;
    std::istringstream iss(s);
    std::string part;
    while (iss >> part) parts.push_back(part);
    return parts;
}

}  // namespace

std::shared_ptr<PooledConnection> ConnectionPool::GetOrCreate(const std::string& location,
                                                               const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    std::lock_guard<std::mutex> lock(mutex_);
    if (auto it = connections_.find(key); it != connections_.end()) return it->second;

    auto pooled = std::make_shared<PooledConnection>(PooledConnection{
        VgiConnection::spawn(SplitWhitespace(location)),
        {},
    });
    VgiCatalogClient catalog(pooled->connection);
    pooled->attach_opaque_data = catalog.Attach(catalog_name).attach_opaque_data;
    connections_[key] = pooled;
    return pooled;
}

}  // namespace vgi_sqlite
