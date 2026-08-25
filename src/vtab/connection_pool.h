// © Copyright 2026 Query Farm LLC - https://query.farm
//
// ConnectionPool: a checkout/release pool of live worker connections
// (each its own spawned process + catalog_attach session), keyed by
// (location, catalog). Owned by the vgi_worker module's client data (see
// RegisterVgiWorkerModule), scoped to one sqlite3* connection's lifetime.
//
// Why checkout/release rather than one shared connection per key (the
// original MVP design): vgi_rpc::RpcClient allows exactly one live call/
// stream at a time *per connection*. A table scan's producer stream stays
// open across the whole scan (every xNext up to eof), so anything else
// that needs the same (location, catalog) worker while that scan is
// in-flight - a second concurrent scan of another table from the same
// catalog, or a scalar function called per-row *during* that scan's
// xColumn - collides with it ("raw RPC client already has an active
// call"), found by testing `SELECT scalar_fn(col) FROM some_table`
// against vgi-fixture-worker. Waiting for the busy connection to free up
// isn't an option either: the scalar-function-during-scan case runs on
// the very same thread that's holding the scan's connection checked out,
// between two of its own xNext calls - it would deadlock, not block
// briefly. Acquire() instead spawns a new physical connection (a new
// worker process + its own catalog_attach) whenever every existing one
// for that key is checked out, and Release() (via Checkout's destructor)
// returns it to an idle pool for the next caller to reuse - so the common
// case (a scalar function called between scans, or repeatedly once no
// scan is contending) still shares one process, and only genuine
// concurrent use pays for an extra one.
//
// MVP: connections are never evicted/health-checked and the idle pool
// never shrinks - a worker process that dies mid-session leaves whichever
// checkouts were using it broken until the db connection is closed and
// reopened, and a session that briefly needed N concurrent connections
// keeps all N spawned for the rest of the session. Reconnect-on-failure
// and idle eviction are production-hardening follow-ups (see the plan).
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "rpc/vgi_connection.h"

namespace vgi_sqlite {

struct PooledConnection {
    VgiConnection connection;
    std::string attach_opaque_data;
};

class ConnectionPool {
public:
    // Move-only RAII handle: exclusive use of one physical connection to
    // (location, catalog) while alive. Returned to that key's idle set for
    // reuse when the Checkout is destroyed (or moved-from). Holding one
    // for a scan's whole duration, or acquiring-and-dropping one per
    // scalar function call, are both expected usage patterns.
    class Checkout {
    public:
        Checkout(Checkout&& other) noexcept { *this = std::move(other); }
        Checkout& operator=(Checkout&& other) noexcept;
        ~Checkout();
        Checkout(const Checkout&) = delete;
        Checkout& operator=(const Checkout&) = delete;

        PooledConnection& operator*() const { return *conn_; }
        PooledConnection* operator->() const { return conn_.get(); }

    private:
        friend class ConnectionPool;
        Checkout(ConnectionPool* pool, std::string key, std::shared_ptr<PooledConnection> conn)
            : pool_(pool), key_(std::move(key)), conn_(std::move(conn)) {}

        ConnectionPool* pool_ = nullptr;
        std::string key_;
        std::shared_ptr<PooledConnection> conn_;
    };

    // Checks out a connection to (location, catalog) for exclusive use,
    // reusing an idle one already attached to that pair if one is
    // available, else spawning and attaching a new one. Throws on
    // spawn/attach failure - the caller (xConnect/xFilter/a scalar
    // function call) surfaces it as a SQLite error.
    Checkout Acquire(const std::string& location, const std::string& catalog_name);

private:
    void ReleaseInternal(const std::string& key, std::shared_ptr<PooledConnection> conn);

    std::mutex mutex_;
    std::map<std::string, std::vector<std::shared_ptr<PooledConnection>>> idle_;  // key: location + "\0" + catalog
};

}  // namespace vgi_sqlite
