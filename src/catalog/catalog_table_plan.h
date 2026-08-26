// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Splits: an optional planning phase, layered on top of the ordinary
// bind->init->tick scan lifecycle, that a table function opts into via
// FunctionInfo.supports_splits (ScanFunction::supports_splits here - see
// catalog_client.h). table_function_plan divides one scan into N
// independently-redeemable, opaque "splits" - InitRequest.split_tokens
// then parameterizes an otherwise-ordinary init to redeem exactly one of
// them. Real distributed engines (Spark, Trino, DataFusion) exist to claim
// and read many splits *concurrently*, which is the entire reason the
// mechanism exists - but nothing about the wire protocol requires
// concurrency: a single reader claiming every split *sequentially* is a
// legal, degenerate consumer, which is exactly what a SQLite vtab (one
// cursor, no worker threads) needs. PlanTableFunctionSplits below is that
// sequential consumer - it does the whole paginated plan RPC loop and
// hands back a flat, ordered list of tokens for TableScanner to redeem
// one at a time.
//
// Ported from (not translated line-for-line, but modeled directly on) vgi
// (the DuckDB extension)'s own InvokeTableFunctionPlan/
// ParseVgiScanPlanPage (src/vgi_catalog_api.cpp) - the only real reference
// consumer of this protocol that exists anywhere, confirmed with the
// protocol's own author (see the plan file's splits status for the
// citation) to degenerate correctly to a single sequential reader. Most
// design decisions were carried over deliberately, not reinvented - one
// was deliberately NOT carried over, flagged by that same author on
// review:
//   - EVERY `next_cursors` entry returned by a response is followed, not
//     just the first. The contract is that enumeration is complete only
//     when every outstanding cursor has returned none; a worker handing
//     back more than one cursor is asking for parallel fan-out, and
//     taking just the first would drop everything reachable from the
//     rest *silently* - missing rows with no error, exactly the failure
//     class splits exists to prevent. vgi's own DuckDB client follows
//     only the first cursor (a deliberate no-fan-out simplification
//     appropriate to ITS scope, since every worker observed there returns
//     0 or 1 today anyway) - but "sequential" doesn't force that same
//     tradeoff here: draining every cursor one at a time (a plain queue,
//     see PlanTableFunctionSplits) costs a few lines and stays entirely
//     single-threaded, so there's no reason to accept the silent-subset
//     risk merely because a thread pool isn't in the picture.
//   - Split disjointness (no split reachable from two different cursors)
//     is a worker obligation, not policed by the client - matching vgi's
//     own reasoning for why it went from "dedup by token" to "trust the
//     contract, do nothing" (a hash-set guard on every scan's plan to
//     police a shape no real client actually emits was reconsidered and
//     dropped in the shipped implementation; the most a client could do
//     with a duplicate is refuse, swapping a worker bug for a different
//     error message).
//   - Plan-level facts (init_opaque_data) are taken from the FIRST
//     response only, keyed on request count rather than on "first
//     response with data" - a legal leading response can carry zero
//     splits and a cursor (a worker still enumerating).
//   - Bounded on total requests and total splits (matching vgi's own
//     1024-page / 2^20-split caps, generalized here to "requests" since
//     multiple cursors can each be individually paginated) - a breach
//     throws rather than silently scanning a partial enumeration, which
//     would return a subset of the table wearing the costume of a
//     successful query.
//   - min_splits/target_split_bytes are never sent: this driver has no
//     concurrency to size splits for (see BuildTableFunctionPlanRequest),
//     matching vgi's own DuckDB client omitting target_split_bytes for
//     the identical reason ("DuckDB has no basis to invent a byte
//     target"). Unlike vgi, this driver doesn't send min_splits=thread_count
//     either, since it never reads more than one split at a time.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rpc/vgi_connection.h"

namespace vgi_sqlite {

// The inner TableFunctionPlanRequest builder. bind_call_bytes/
// bind_opaque_data are the SAME bind() this table's ordinary scan already
// performed - a plan is scoped to one bind, exactly like init is.
// projection_ids/pushdown_filters carry the same pushdown this driver
// already computed for an ordinary scan (see vgi_vtab.cpp's xFilter) -
// splits doesn't change what xBestIndex/xFilter decide to push down, only
// how the resulting scan is redeemed. cursor is the pagination cursor (
// empty for the first page). filters_complete is always sent true: this
// driver plans from the same static, already-fully-resolved filter set
// an ordinary init would use, never a dynamic join-key filter arriving
// after the fact (join_keys is always empty/null here, matching vgi's own
// DuckDB client - dynamic filter pushdown into split planning isn't
// implemented on either the filter-encoding or the splits side of this
// driver).
std::shared_ptr<arrow::RecordBatch> BuildTableFunctionPlanRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& bind_opaque_data,
    const std::vector<int64_t>& projection_ids, const std::optional<std::string>& pushdown_filters,
    const std::vector<uint8_t>& cursor);

// The result of planning a whole scan into splits, sequentially, in this
// process - see this file's comment.
struct TableFunctionPlan {
    // Redemption tokens, in the order the worker's pages returned them -
    // this driver reads splits in that order, not by any priority/size
    // heuristic (it has no scheduler to feed one to). Legally empty: a
    // fully-pruned scan plans zero splits, which must produce an empty
    // result, not an error (see vgi's own ParseVgiScanPlanPage comment).
    std::vector<std::vector<uint8_t>> split_tokens;
    // Replaces bind_.opaque_data as every redeeming init's bind_opaque_data
    // - a plan is itself a kind of re-bind, and its own opaque_data (not
    // the original bind's) is what every split's init must carry. Taken
    // from the first page only (see this file's comment).
    std::vector<uint8_t> init_opaque_data;
};

// Runs the full paginated table_function_plan RPC loop against `connection`
// and returns every split token collected, in order. Throws (a
// std::runtime_error, this driver's uniform exception convention) if the
// worker's pagination doesn't terminate within bounded pages/splits, or
// if a returned ScanSplit can't be parsed / has no token - a worker
// bypassing the token-stamping framework is itself a protocol violation,
// not something to paper over.
TableFunctionPlan PlanTableFunctionSplits(VgiConnection& connection, const std::vector<uint8_t>& bind_call_bytes,
                                          const std::vector<uint8_t>& bind_opaque_data,
                                          const std::vector<int64_t>& projection_ids,
                                          const std::optional<std::string>& pushdown_filters);

}  // namespace vgi_sqlite
