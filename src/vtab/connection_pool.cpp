// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/connection_pool.h"

#include <optional>
#include <sstream>
#include <utility>
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

ConnectionPool::Checkout& ConnectionPool::Checkout::operator=(Checkout&& other) noexcept {
    if (this != &other) {
        // Release whatever this Checkout currently holds before taking
        // over other's - matches std::unique_ptr's move-assign contract.
        if (pool_ && conn_) pool_->ReleaseInternal(key_, std::move(conn_));
        pool_ = other.pool_;
        key_ = std::move(other.key_);
        conn_ = std::move(other.conn_);
        other.pool_ = nullptr;
        other.conn_.reset();
    }
    return *this;
}

ConnectionPool::Checkout::~Checkout() {
    if (pool_ && conn_) pool_->ReleaseInternal(key_, std::move(conn_));
}

ConnectionPool::Checkout ConnectionPool::Acquire(const std::string& location,
                                                  const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    std::optional<std::string> cached_attach_opaque_data;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = idle_.find(key);
        if (it != idle_.end() && !it->second.empty()) {
            auto conn = std::move(it->second.back());
            it->second.pop_back();
            return Checkout(this, key, std::move(conn));
        }
        if (auto shared_it = shared_attach_opaque_data_.find(key); shared_it != shared_attach_opaque_data_.end()) {
            cached_attach_opaque_data = shared_it->second;
        }
    }
    // No idle connection for this key - spawn a new one, outside the lock
    // (spawning a subprocess, and round-tripping catalog_attach when this
    // is genuinely the first connection for this key, can both take a
    // while; no need to block every other Acquire()/Release() on this
    // pool while it happens).
    auto pooled = std::make_shared<PooledConnection>(PooledConnection{
        VgiConnection::spawn(SplitWhitespace(location)),
        {},
    });
    if (cached_attach_opaque_data) {
        // Not the first connection for this key - reuse the attachment
        // identity the first one minted instead of calling catalog_attach
        // again (which would mint an unrelated one on a worker whose
        // attach_opaque_data carries real per-attach state) - see this
        // header's file comment.
        pooled->attach_opaque_data = *cached_attach_opaque_data;
    } else {
        VgiCatalogClient catalog(pooled->connection);
        pooled->attach_opaque_data = catalog.Attach(catalog_name).attach_opaque_data;
        std::lock_guard<std::mutex> lock(mutex_);
        // Another thread may have raced this and already cached one first
        // (this pool method is otherwise unused single-threaded today, but
        // don't assume it stays that way) - first writer wins, so every
        // connection for this key agrees on one identity.
        shared_attach_opaque_data_.try_emplace(key, pooled->attach_opaque_data);
        pooled->attach_opaque_data = shared_attach_opaque_data_[key];
    }
    return Checkout(this, key, std::move(pooled));
}

void ConnectionPool::ReleaseInternal(const std::string& key, std::shared_ptr<PooledConnection> conn) {
    std::lock_guard<std::mutex> lock(mutex_);
    idle_[key].push_back(std::move(conn));
}

}  // namespace vgi_sqlite
