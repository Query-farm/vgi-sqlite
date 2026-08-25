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
// A checkout whose scope is exited via a propagating exception is
// discarded instead of recycled into idle_ - see Checkout::Release()'s
// std::uncaught_exceptions() check. Every call site already wraps its use
// of a checked-out connection in a try/catch that turns an exception into
// a SQLite error (a `bad function name`-type application error looks the
// same as a truly dead connection from here), so this can't distinguish
// "the worker process actually crashed" from "the call was rejected for
// an unrelated reason" - it deliberately errs toward discarding, since a
// spurious extra respawn costs a little work but a truly dead connection
// silently recycled forever (poisoning every future call on that key)
// costs correctness. A session that briefly needed N concurrent
// connections doesn't keep all N spawned for the rest of the session,
// though: every Acquire() call first prunes any idle connection across
// *every* key that's been sitting unused longer than kIdleTimeout (see
// PruneStaleIdleLocked) - a connection released back to idle_ that's
// never reused within that window gets its worker process torn down (via
// PooledConnection's own shared_ptr reaching zero references) the next
// time anything, anywhere in the pool, calls Acquire(). This is
// opportunistic, not a background timer - a key that's checked out once
// and then never touched again for the rest of the session (and no other
// key ever calls Acquire() again either) keeps its idle connections alive
// until the whole pool is destroyed, since nothing triggers the check. A
// real timer thread would close that gap but adds real complexity (thread
// lifecycle tied to the pool's own, itself tied to a sqlite3_module's
// client-data destructor) for a case unlikely to matter in practice -
// accepted here rather than built.
//
// attach_opaque_data is minted ONCE per key, not once per physical
// connection: the first successful Acquire() for a (location, catalog)
// pair does a real catalog_attach and caches its attach_opaque_data;
// every connection spawned afterward for that same key (when concurrency
// needs a second physical process) reuses that cached value directly and
// skips catalog_attach entirely, rather than minting its own. Found by
// testing a real writable worker: vgi-fixture-worker's simple_writable
// fixture derives its backing SQLite file's path from
// attach_opaque_data's own bytes (a fresh random UUID each catalog_attach
// call), so two physical connections that each independently attached
// ended up looking at two completely unrelated, empty databases - an
// INSERT on one connection was invisible to a SELECT on the other, with
// no error anywhere to say so. Matches how VGI's own reference engine
// (DuckDB's extension) is documented to work: attach_opaque_data is
// minted once per ATTACH and threaded through every subsequent call
// regardless of which of the engine's own worker-pool processes ends up
// serving it - see vgi-c++'s CLAUDE.md ("A worker pool hands out a
// different process per RPC, so...per-attachment state...belongs in
// FunctionStorage, not in a member") and the design intent documented on
// InitRequest.execution_id/substream_id (explicitly built to survive an
// HTTP load balancer routing one execution's calls to different backend
// processes). A worker whose catalog_attach does real *per-process* setup
// a second, attach-skipping connection would miss (e.g. one backed by a
// private in-process session dict rather than state derived from/keyed by
// the opaque bytes themselves) isn't safe under this reuse - a narrower,
// now-explicit version of the same caveat ScalarFunctionCaller's file
// comment already documents about attach_opaque_data's portability.
//
// Transactions (Begin/Commit/RollbackTransaction, CurrentTransactionOpaqueData)
// apply the identical "one value per key, shared across every physical
// connection for that key" pattern to VGI's transaction_opaque_data, for
// the same underlying reason plus a second problem specific to
// transactions: SQLite calls a vtab module's xBegin/xCommit/xRollback once
// per *vtab instance* per transaction (three vgi_worker tables from the
// same catalog in one SQL transaction get three xBegin calls), but VGI's
// catalog_transaction_begin/commit/rollback is scoped to one *(location,
// catalog) attachment*, not one table. Begin/Commit/RollbackTransaction
// ref-count per key: only the first Begin for a key actually calls
// catalog_transaction_begin (skipped entirely if that catalog's
// CatalogAttachResult.supports_transactions is false - cached alongside
// attach_opaque_data, see AttachInfo), and only the Commit/Rollback that
// brings the ref count back to zero actually calls catalog_transaction_
// commit/rollback. Every operation performed *during* the transaction
// (TableScanner::Bind, TableWriter::Write, ...) reads
// CurrentTransactionOpaqueData(key) and threads it into its own bind/init
// call, regardless of which physical connection that particular checkout
// happens to be - not the specific connection Begin used, matching the
// same reuse-across-connections precedent attach_opaque_data already set.
//
// Not implemented: SQLite's xSavepoint/xRelease/xRollbackTo (SAVEPOINT
// nesting within one transaction) - VGI's transaction RPC surface has no
// savepoint concept at all (only whole-transaction begin/commit/rollback),
// and leaving these three vtab callbacks null is safe: SQLite simply
// doesn't offer/use the savepoint mechanism for a vtab that doesn't
// implement them (confirmed by every INSERT/UPDATE/DELETE test in this
// repo passing before transactions existed at all, all of them implicitly
// wrapped in SQLite's own per-statement transaction machinery with these
// three left null the whole time) - ordinary single- and multi-statement
// writes don't need them; only an explicit SQL SAVEPOINT would, and this
// driver doesn't support that.
#pragma once

#include <chrono>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "rpc/vgi_connection.h"

namespace vgi_sqlite {

struct PooledConnection {
    VgiConnection connection;
    std::string attach_opaque_data;
    // Set by ReleaseInternal when this connection goes idle; read (and
    // acted on) by PruneStaleIdleLocked. Meaningless while checked out.
    std::chrono::steady_clock::time_point idle_since;
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
            : pool_(pool),
              key_(std::move(key)),
              conn_(std::move(conn)),
              uncaught_at_construction_(std::uncaught_exceptions()) {}

        // Recycle into idle_ on a normal exit, or drop (never call
        // ReleaseInternal - just let conn_'s reference count fall to zero
        // and destroy the connection) if an exception is propagating
        // through this Checkout's scope - see the file comment on why
        // that's the right default even though it can't distinguish a
        // truly dead connection from an unrelated application-level
        // error. Shared by the destructor and move-assignment.
        void ReleaseOrDiscard();

        ConnectionPool* pool_ = nullptr;
        std::string key_;
        std::shared_ptr<PooledConnection> conn_;
        int uncaught_at_construction_ = 0;
    };

    // Checks out a connection to (location, catalog) for exclusive use,
    // reusing an idle one already attached to that pair if one is
    // available, else spawning and attaching a new one. Throws on
    // spawn/attach failure - the caller (xConnect/xFilter/a scalar
    // function call) surfaces it as a SQLite error.
    Checkout Acquire(const std::string& location, const std::string& catalog_name);

    // Begin/Commit/RollbackTransaction and CurrentTransactionOpaqueData:
    // see the file comment above for the full design (shared,
    // ref-counted per (location, catalog) key, coordinating across
    // however many vgi_worker table instances from that pair xBegin gets
    // called on for one SQL transaction). Called from vgi_vtab.cpp's
    // xBegin/xCommit/xRollback. All three throw on RPC failure; the
    // caller surfaces it as SQLite's own xBegin/xSync/xCommit/xRollback
    // error contract expects.
    void BeginTransaction(const std::string& location, const std::string& catalog_name);
    void CommitTransaction(const std::string& location, const std::string& catalog_name);
    void RollbackTransaction(const std::string& location, const std::string& catalog_name);

    // The active transaction's opaque data for (location, catalog), or
    // nullopt if no transaction is active there right now (including:
    // never began one because that catalog doesn't support transactions).
    // Read by TableScanner::Bind/TableWriter::Write's callers to thread
    // into their own bind calls - see the file comment.
    std::optional<std::vector<uint8_t>> CurrentTransactionOpaqueData(const std::string& location,
                                                                       const std::string& catalog_name);

private:
    void ReleaseInternal(const std::string& key, std::shared_ptr<PooledConnection> conn);

    // Drops every idle connection (any key) that's been sitting unused
    // longer than kIdleTimeout - see the file comment. Called with
    // mutex_ already held, at the start of every Acquire().
    void PruneStaleIdleLocked();

    static constexpr std::chrono::minutes kIdleTimeout{5};

    // Cached from the first successful Acquire() for a key - see the file
    // comment on why every later connection for that key reuses this
    // rather than re-attaching.
    struct AttachInfo {
        std::string attach_opaque_data;
        bool supports_transactions = false;
    };

    struct TransactionState {
        int ref_count = 0;
        // nullopt either "no transaction began yet for this key" or
        // "began one, but that catalog doesn't support transactions so
        // there's genuinely no transaction_opaque_data" - both mean
        // CurrentTransactionOpaqueData reports nullopt either way, and
        // Commit/Rollback skip their RPC either way, so the two cases
        // don't need to be distinguished.
        std::optional<std::vector<uint8_t>> transaction_opaque_data;
    };

    std::mutex mutex_;
    std::map<std::string, std::vector<std::shared_ptr<PooledConnection>>> idle_;  // key: location + "\0" + catalog
    std::map<std::string, AttachInfo> attach_info_;                               // same key; set on the first Acquire()
    std::map<std::string, TransactionState> transactions_;                        // same key; present only while active
};

}  // namespace vgi_sqlite
