// © Copyright 2026 Query Farm LLC - https://query.farm
//
// AggregateCaller: drives one VGI aggregate function's bind -> update* ->
// finalize -> destructor lifecycle for exactly one SQLite aggregate
// context (one GROUP BY group, or the single implicit group of a
// whole-table aggregate - SQLite already creates a fresh context per
// group, so one AggregateCaller instance per context is the natural,
// correct mapping - see aggregate_requests.h's file comment on why VGI's
// own group_id concept doesn't need replicating client-side: this caller
// always uses group_id 0 within its own execution_id, which is itself
// only ever bound to one context/group).
//
// Holds ONE ConnectionPool checkout for its ENTIRE lifetime (bind through
// finalize+destructor) - unlike ScalarFunctionCaller/TableWriter, which
// open a fresh connection per call. Deliberate: an aggregate's
// execution_id-keyed worker-side state accumulates across many Step()
// calls (one per input row), and unlike attach_opaque_data (confirmed
// NOT portable across independently-spawned connections by default - see
// connection_pool.h's file comment), this driver doesn't rely on
// execution_id being safe to replay against a different physical
// connection/process than the one bind produced it on, even though VGI's
// own design intent (per vgi-c++'s CLAUDE.md: "Execution ids must be
// unique across processes, since that store is shared by every worker of
// the uid") suggests it's meant to be portable in a distributed
// deployment - staying on one connection for the whole accumulation
// sidesteps needing to trust that for every worker this driver might talk
// to, matching the same conservative choice already made for
// TableScanner's whole-scan connection.
//
// Combine is never called: it exists to merge parallel workers' partial
// states, and this driver drives every aggregate from one thread on one
// connection - a single-threaded serial caller can skip it entirely
// (confirmed by reading vgi-python's own single-threaded aggregate_function()
// driver, which never calls combine either).
//
// Windowed aggregates (SQL OVER clauses) are a structurally different RPC
// family (aggregate_window*) with no incremental step/inverse model at
// all - out of scope here; this class only drives the plain
// bind/update/finalize cycle GROUP BY (and whole-table) aggregates use.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vtab/connection_pool.h"

namespace vgi_sqlite {

class AggregateCaller {
public:
    AggregateCaller(ConnectionPool& pool, std::string location, std::string catalog_name,
                     std::string function_name, int num_args, std::optional<std::string> schema_name);

    // Best-effort aggregate_destructor call for this caller's one group,
    // mirroring TableScanner's stream-close destructor - cleans up
    // worker-side state even if Finalize() was never reached (e.g. a
    // SQLite error abandons the aggregate context mid-scan).
    ~AggregateCaller();

    int num_args() const { return num_args_; }

    // Accumulates one row into this caller's group (see the file comment
    // on why that's always group_id 0). Binds lazily on the first call,
    // locking in argument types from `args`' actual values - same
    // reasoning as ScalarFunctionCaller (FunctionInfo.arguments can't be
    // trusted). `args` must have exactly num_args() elements.
    void Step(const std::vector<std::shared_ptr<arrow::Scalar>>& args);

    // Produces the final result for this caller's one group. Must be
    // called at most once, after at least one Step() (an aggregate over
    // zero rows never binds at all - see extension.cpp's
    // AggregateFinalBridge for how that's handled: no caller exists yet
    // to call this on).
    std::shared_ptr<arrow::RecordBatch> Finalize();

private:
    void EnsureBound(const std::vector<std::shared_ptr<arrow::Scalar>>& args);

    ConnectionPool& pool_;
    std::string location_;
    std::string catalog_name_;
    std::string function_name_;
    int num_args_;
    std::optional<std::string> schema_name_;

    bool bound_ = false;
    bool finalized_ = false;
    std::shared_ptr<arrow::Schema> arg_types_;  // locked in on first Step(); "col_N" names
    std::optional<ConnectionPool::Checkout> checkout_;
    std::vector<uint8_t> execution_id_;
    std::shared_ptr<arrow::Schema> output_schema_;
};

}  // namespace vgi_sqlite
