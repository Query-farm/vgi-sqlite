// © Copyright 2026 Query Farm LLC - https://query.farm
//
// ConnectionPool: one live worker connection (and one catalog_attach
// session) shared across every vgi_worker table from the same
// (location, catalog) pair, so vgi_attach()-ing a catalog with dozens of
// tables spawns one worker process, not one per table. Owned by the
// vgi_worker module's client data (see RegisterVgiWorkerModule), scoped
// to one sqlite3* connection's lifetime.
//
// MVP: a straight cache, never evicted/health-checked - a worker process
// that dies mid-session leaves every table sharing it broken until the db
// connection is closed and reopened. Reconnect-on-failure and idle
// eviction are production-hardening follow-ups (see the plan).
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "rpc/vgi_connection.h"

namespace vgi_sqlite {

struct PooledConnection {
    VgiConnection connection;
    std::string attach_opaque_data;
};

class ConnectionPool {
public:
    // Returns the shared connection for (location, catalog), spawning and
    // attaching one if this is the first table from that pair seen so
    // far. Throws on spawn/attach failure - the caller (xConnect/xFilter)
    // surfaces it as a SQLite error.
    std::shared_ptr<PooledConnection> GetOrCreate(const std::string& location,
                                                   const std::string& catalog_name);

private:
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<PooledConnection>> connections_;  // key: location + "\0" + catalog
};

}  // namespace vgi_sqlite
