// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vtab/connection_pool.h"

#include <algorithm>
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

void ConnectionPool::Checkout::ReleaseOrDiscard() {
    if (!pool_ || !conn_) return;
    if (std::uncaught_exceptions() > uncaught_at_construction_) {
        conn_.reset();
    } else {
        pool_->ReleaseInternal(key_, std::move(conn_));
    }
}

ConnectionPool::Checkout& ConnectionPool::Checkout::operator=(Checkout&& other) noexcept {
    if (this != &other) {
        // Release (or discard) whatever this Checkout currently holds
        // before taking over other's - matches std::unique_ptr's
        // move-assign contract.
        ReleaseOrDiscard();
        pool_ = other.pool_;
        key_ = std::move(other.key_);
        conn_ = std::move(other.conn_);
        uncaught_at_construction_ = other.uncaught_at_construction_;
        other.pool_ = nullptr;
        other.conn_.reset();
    }
    return *this;
}

ConnectionPool::Checkout::~Checkout() { ReleaseOrDiscard(); }

void ConnectionPool::PruneStaleIdleLocked() {
    auto now = std::chrono::steady_clock::now();
    for (auto& [key, conns] : idle_) {
        (void)key;
        conns.erase(std::remove_if(conns.begin(), conns.end(),
                                   [&](const std::shared_ptr<PooledConnection>& c) {
                                       return now - c->idle_since > kIdleTimeout;
                                   }),
                    conns.end());
    }
}

ConnectionPool::Checkout ConnectionPool::Acquire(const std::string& location,
                                                  const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    std::optional<AttachInfo> cached_attach_info;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        PruneStaleIdleLocked();
        auto it = idle_.find(key);
        if (it != idle_.end() && !it->second.empty()) {
            auto conn = std::move(it->second.back());
            it->second.pop_back();
            return Checkout(this, key, std::move(conn));
        }
        if (auto info_it = attach_info_.find(key); info_it != attach_info_.end()) {
            cached_attach_info = info_it->second;
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
        {},
    });
    if (cached_attach_info) {
        // Not the first connection for this key - reuse the attachment
        // identity the first one minted instead of calling catalog_attach
        // again (which would mint an unrelated one on a worker whose
        // attach_opaque_data carries real per-attach state) - see this
        // header's file comment.
        pooled->attach_opaque_data = cached_attach_info->attach_opaque_data;
    } else {
        VgiCatalogClient catalog(pooled->connection);
        auto attach = catalog.Attach(catalog_name);
        pooled->attach_opaque_data = attach.attach_opaque_data;
        std::lock_guard<std::mutex> lock(mutex_);
        // Another thread may have raced this and already cached one first
        // (this pool method is otherwise unused single-threaded today, but
        // don't assume it stays that way) - first writer wins, so every
        // connection for this key agrees on one identity.
        attach_info_.try_emplace(key, AttachInfo{attach.attach_opaque_data, attach.supports_transactions});
        pooled->attach_opaque_data = attach_info_[key].attach_opaque_data;
    }
    return Checkout(this, key, std::move(pooled));
}

void ConnectionPool::ReleaseInternal(const std::string& key, std::shared_ptr<PooledConnection> conn) {
    conn->idle_since = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    idle_[key].push_back(std::move(conn));
}

void ConnectionPool::BeginTransaction(const std::string& location, const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    bool need_begin = false;
    bool supports_transactions = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& state = transactions_[key];
        need_begin = (state.ref_count == 0);
        ++state.ref_count;
        // attach_info_ is guaranteed populated by now: xConnect (which
        // every vgi_worker table runs before it's usable at all, via
        // ConnectImpl's own Acquire()+TableGet()) always resolves it
        // before any xBegin on that table could possibly fire.
        if (auto it = attach_info_.find(key); it != attach_info_.end()) {
            supports_transactions = it->second.supports_transactions;
        }
    }
    // Gated on CatalogAttachResult.supports_transactions (see the file
    // comment) - a catalog that doesn't support transactions never gets a
    // catalog_transaction_begin call at all; CurrentTransactionOpaqueData
    // stays nullopt for it regardless of ref_count, so every bind made
    // "during" this no-op transaction still passes nullopt, exactly as if
    // no transaction existed.
    if (!need_begin || !supports_transactions) return;

    try {
        // Acquire()/release a checkout just for this one RPC, outside any
        // lock - same rationale as Acquire()'s own spawn/attach path.
        auto checkout = Acquire(location, catalog_name);
        VgiCatalogClient catalog(checkout->connection);
        auto transaction_opaque_data = catalog.TransactionBegin(checkout->attach_opaque_data);
        std::lock_guard<std::mutex> lock(mutex_);
        transactions_[key].transaction_opaque_data = std::move(transaction_opaque_data);
    } catch (...) {
        // Undo the ref-count bump - a failed Begin must not leave a later
        // Commit/Rollback believing a transaction is active when none
        // ever actually started.
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto it = transactions_.find(key); it != transactions_.end()) {
            if (--it->second.ref_count <= 0) transactions_.erase(it);
        }
        throw;
    }
}

void ConnectionPool::CommitTransaction(const std::string& location, const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    bool need_commit = false;
    std::optional<std::vector<uint8_t>> transaction_opaque_data;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = transactions_.find(key);
        if (it == transactions_.end()) return;  // no active transaction - defensively a no-op
        if (--it->second.ref_count <= 0) {
            need_commit = true;
            transaction_opaque_data = std::move(it->second.transaction_opaque_data);
            transactions_.erase(it);
        }
    }
    // Not the last participant to commit, or that catalog never actually
    // began a transaction (doesn't support them) - nothing more to do.
    if (!need_commit || !transaction_opaque_data) return;

    auto checkout = Acquire(location, catalog_name);
    VgiCatalogClient catalog(checkout->connection);
    catalog.TransactionCommit(checkout->attach_opaque_data, *transaction_opaque_data);
}

void ConnectionPool::RollbackTransaction(const std::string& location, const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    bool need_rollback = false;
    std::optional<std::vector<uint8_t>> transaction_opaque_data;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = transactions_.find(key);
        if (it == transactions_.end()) return;
        if (--it->second.ref_count <= 0) {
            need_rollback = true;
            transaction_opaque_data = std::move(it->second.transaction_opaque_data);
            transactions_.erase(it);
        }
    }
    if (!need_rollback || !transaction_opaque_data) return;

    auto checkout = Acquire(location, catalog_name);
    VgiCatalogClient catalog(checkout->connection);
    catalog.TransactionRollback(checkout->attach_opaque_data, *transaction_opaque_data);
}

std::optional<std::vector<uint8_t>> ConnectionPool::CurrentTransactionOpaqueData(
    const std::string& location, const std::string& catalog_name) {
    const std::string key = location + '\0' + catalog_name;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = transactions_.find(key);
    if (it == transactions_.end()) return std::nullopt;
    return it->second.transaction_opaque_data;
}

}  // namespace vgi_sqlite
