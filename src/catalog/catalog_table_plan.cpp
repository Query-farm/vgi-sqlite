// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/catalog_table_plan.h"

#include <stdexcept>

#include <arrow/api.h>

#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

void check_ok(const arrow::Status& status, const char* what) {
    if (!status.ok()) throw std::runtime_error(std::string(what) + ": " + status.ToString());
}

template <typename BuilderType>
std::shared_ptr<arrow::Array> finish(BuilderType& builder, const char* what) {
    auto result = builder.Finish();
    if (!result.ok()) throw std::runtime_error(std::string("finishing ") + what + ": " + result.status().ToString());
    return result.ValueUnsafe();
}

std::shared_ptr<arrow::Array> binary_scalar(const std::vector<uint8_t>& bytes) {
    arrow::BinaryBuilder b;
    check_ok(b.Append(bytes.data(), static_cast<int32_t>(bytes.size())), "append binary");
    return finish(b, "binary");
}

std::shared_ptr<arrow::Array> null_binary() {
    arrow::BinaryBuilder b;
    check_ok(b.AppendNull(), "append null binary");
    return finish(b, "null binary");
}

std::shared_ptr<arrow::Array> null_large_binary() {
    arrow::LargeBinaryBuilder b;
    check_ok(b.AppendNull(), "append null large_binary");
    return finish(b, "null large_binary");
}

std::shared_ptr<arrow::Array> null_int64() {
    arrow::Int64Builder b;
    check_ok(b.AppendNull(), "append null int64");
    return finish(b, "null int64");
}

std::shared_ptr<arrow::Array> null_dictionary(const std::shared_ptr<arrow::DataType>& dict_type,
                                               const std::vector<std::string>& dictionary_values) {
    arrow::Int16Builder index_builder;
    check_ok(index_builder.AppendNull(), "append null dict index");
    auto index_arr = finish(index_builder, "null_dict_index");
    arrow::StringBuilder dict_builder;
    for (const auto& v : dictionary_values) check_ok(dict_builder.Append(v), "append dict value");
    auto dict_arr = finish(dict_builder, "dict_values");
    auto result = arrow::DictionaryArray::FromArrays(dict_type, index_arr, dict_arr);
    check_ok(result.status(), "creating null dictionary array");
    return result.ValueUnsafe();
}

}  // namespace

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionPlanRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& bind_opaque_data,
    const std::vector<int64_t>& projection_ids, const std::optional<std::string>& pushdown_filters,
    const std::vector<uint8_t>& cursor) {
    auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
    // Field order/nullability ported verbatim from vgi's own
    // BuildTableFunctionPlanRequest (vgi_rpc_types.cpp) - see this repo's
    // scan_requests.h file comment on why that match is load-bearing, not
    // cosmetic.
    auto schema = arrow::schema({
        arrow::field("bind_call", arrow::binary(), false),
        arrow::field("bind_opaque_data", arrow::binary(), true),
        arrow::field("projection_ids", arrow::list(arrow::int64()), true),
        arrow::field("pushdown_filters", arrow::large_binary(), true),
        arrow::field("join_keys", arrow::list(arrow::large_binary()), true),
        arrow::field("row_limit", arrow::int64(), true),
        arrow::field("target_split_bytes", arrow::int64(), true),
        arrow::field("min_splits", arrow::int64(), true),
        arrow::field("max_splits_per_response", arrow::int64(), true),
        arrow::field("cursor", arrow::binary(), true),
        arrow::field("refined_filters", arrow::large_binary(), true),
        arrow::field("filters_complete", arrow::boolean(), false),
        arrow::field("start_position", arrow::binary(), true),
        arrow::field("end_position", arrow::binary(), true),
        arrow::field("order_by_column_name", arrow::utf8(), true),
        arrow::field("order_by_direction", dict_type, true),
        arrow::field("order_by_null_order", dict_type, true),
        arrow::field("order_by_limit", arrow::int64(), true),
        arrow::field("tablesample_percentage", arrow::float64(), true),
        arrow::field("tablesample_seed", arrow::int64(), true),
    });

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.push_back(binary_scalar(bind_call_bytes));
    arrays.push_back(bind_opaque_data.empty() ? null_binary() : binary_scalar(bind_opaque_data));
    {
        auto values = std::make_shared<arrow::Int64Builder>();
        arrow::ListBuilder list(arrow::default_memory_pool(), values);
        if (projection_ids.empty()) {
            check_ok(list.AppendNull(), "append null projection_ids");
        } else {
            check_ok(list.Append(), "start projection_ids");
            for (auto id : projection_ids) check_ok(values->Append(id), "append projection id");
        }
        arrays.push_back(finish(list, "projection_ids"));
    }
    {
        arrow::LargeBinaryBuilder b;
        if (pushdown_filters) {
            check_ok(b.Append(pushdown_filters->data(), static_cast<int64_t>(pushdown_filters->size())),
                     "append pushdown_filters");
        } else {
            check_ok(b.AppendNull(), "append null pushdown_filters");
        }
        arrays.push_back(finish(b, "pushdown_filters"));
    }
    {
        // join_keys: always null - a plan is built from static filters
        // only; dynamic join-key pushdown into split planning isn't
        // implemented (see this file's header comment).
        auto values = std::make_shared<arrow::LargeBinaryBuilder>();
        arrow::ListBuilder list(arrow::default_memory_pool(), values);
        check_ok(list.AppendNull(), "append null join_keys");
        arrays.push_back(finish(list, "join_keys"));
    }
    arrays.push_back(null_int64());  // row_limit - not computed for a plan request (see xBestIndex's LIMIT-pushdown
                                      // scoping; threading it through split planning is unimplemented)
    arrays.push_back(null_int64());  // target_split_bytes - this driver has no byte budget to invent
    arrays.push_back(null_int64());  // min_splits - this driver never reads more than one split at a time,
                                      // so there's no concurrency-derived floor to send (unlike vgi's own
                                      // DuckDB client, which sends its thread count)
    arrays.push_back(null_int64());  // max_splits_per_response - no pagination-size preference; take whatever
                                      // page size the worker chooses
    arrays.push_back(cursor.empty() ? null_binary() : binary_scalar(cursor));
    arrays.push_back(null_large_binary());  // refined_filters - see join_keys
    {
        // filters_complete: always true - this driver plans from the same
        // static, already-fully-resolved filter set an ordinary init
        // would use; holding splits back awaiting a narrower filter that
        // will never arrive would stall the scan forever.
        arrow::BooleanBuilder b;
        check_ok(b.Append(true), "append filters_complete");
        arrays.push_back(finish(b, "filters_complete"));
    }
    arrays.push_back(null_binary());  // start_position - no streaming/incremental scan support
    arrays.push_back(null_binary());  // end_position
    arrays.push_back(arrow::MakeArrayOfNull(arrow::utf8(), 1).ValueOrDie());  // order_by_column_name
    arrays.push_back(null_dictionary(dict_type, {"ASC", "DESC"}));                  // order_by_direction
    arrays.push_back(null_dictionary(dict_type, {"NULLS_FIRST", "NULLS_LAST"}));    // order_by_null_order
    arrays.push_back(null_int64());                                                 // order_by_limit
    {
        arrow::DoubleBuilder b;
        check_ok(b.AppendNull(), "append null tablesample_percentage");
        arrays.push_back(finish(b, "tablesample_percentage"));
    }
    arrays.push_back(null_int64());  // tablesample_seed

    return arrow::RecordBatch::Make(schema, 1, arrays);
}

namespace {

// One page's worth of a plan response - mirrors vgi's own
// ParseVgiScanPlanPage (vgi_catalog_api.cpp) closely, trimmed to what a
// sequential single-reader client needs (drops locations/partitioning/
// sort_order/cache_max_age_seconds/estimated_* - real fields this driver
// simply has no consumer for yet, matching the "no live consumer" test
// the protocol design itself was built against).
struct PlanPage {
    std::vector<std::vector<uint8_t>> tokens;
    // Every cursor this page returned, not just the first - see
    // PlanTableFunctionSplits's comment on why all of them must be
    // followed. Empty = this cursor's enumeration is exhausted.
    std::vector<std::vector<uint8_t>> next_cursors;
    std::vector<uint8_t> init_opaque_data;
};

PlanPage ParsePlanPage(const std::shared_ptr<arrow::RecordBatch>& batch) {
    PlanPage page;
    if (!batch || batch->num_rows() == 0) return page;

    for (const auto& raw : wire::get_binary_list(batch, "splits")) {
        // Each entry is a serialized ScanSplit. What gets redeemed is its
        // `token` field - the framework-stamped envelope - never the
        // worker's raw payload (inside the envelope, unverifiable on its
        // own) and never the whole record (everything else in ScanSplit -
        // estimates, partition bounds, statistics - is for a bin-packing
        // engine this driver isn't).
        auto split_batch = wire::decode_ipc(raw);
        if (!split_batch || split_batch->num_rows() == 0) {
            throw std::runtime_error("worker returned an unparseable ScanSplit");
        }
        auto token = wire::get_optional_binary(split_batch, "token");
        if (!token) {
            throw std::runtime_error(
                "worker returned a ScanSplit with no token; the framework stamps this, so an absent "
                "token means the worker bypassed it");
        }
        page.tokens.emplace_back(token->begin(), token->end());
    }

    // next_cursors: every entry must be followed, not just the first. The
    // contract is that enumeration is complete only when EVERY outstanding
    // cursor has returned none - a worker returning more than one is
    // asking for parallel fan-out, and taking just the first would drop
    // everything reachable from the rest *silently*, as missing rows with
    // no error: exactly the failure class splits exists to prevent.
    // Sequential doesn't require real concurrency to stay correct here -
    // draining every cursor one at a time (see PlanTableFunctionSplits)
    // costs a few lines and closes the gap entirely, so there's no reason
    // to accept the silent-subset risk merely because a thread pool isn't
    // in the picture (unlike vgi's own DuckDB client, which follows only
    // the first cursor as a deliberate no-fan-out tradeoff appropriate to
    // its own serial-pagination scope - this driver has no comparable
    // reason to leave that gap open).
    for (const auto& c : wire::get_binary_list(batch, "next_cursors")) {
        page.next_cursors.emplace_back(c.begin(), c.end());
    }

    if (auto opaque = wire::get_optional_binary(batch, "init_opaque_data")) {
        page.init_opaque_data.assign(opaque->begin(), opaque->end());
    }
    return page;
}

}  // namespace

TableFunctionPlan PlanTableFunctionSplits(VgiConnection& connection, const std::vector<uint8_t>& bind_call_bytes,
                                          const std::vector<uint8_t>& bind_opaque_data,
                                          const std::vector<int64_t>& projection_ids,
                                          const std::optional<std::string>& pushdown_filters) {
    // Bounds mirror vgi's own InvokeTableFunctionPlan: generous, and exist
    // only to turn a worker that never terminates its enumeration into a
    // clear error instead of an unbounded hang or an unbounded buffer -
    // not to second-guess a legitimately large plan. kMaxRequests bounds
    // the TOTAL number of table_function_plan calls across every cursor
    // this enumeration ever follows (not per-cursor), since a worker
    // handing back many parallel cursors, each itself paginated, could
    // otherwise multiply page counts unboundedly even with each individual
    // cursor chain staying short.
    constexpr int kMaxRequests = 1024;
    constexpr size_t kMaxSplits = 1u << 20;

    TableFunctionPlan plan;
    // Every outstanding cursor must be followed to completion - see
    // ParsePlanPage's comment on next_cursors. A plain queue, drained
    // depth-first (order doesn't matter: this driver only needs the flat,
    // union set of every token reachable from any cursor, not an ordering
    // between cursors' own results) - seeded with one empty cursor for the
    // initial request.
    std::vector<std::vector<uint8_t>> pending_cursors = {{}};
    int requests = 0;
    bool first_response = true;
    while (!pending_cursors.empty()) {
        if (requests >= kMaxRequests) {
            throw std::runtime_error("table_function_plan exceeded the request cap (" +
                                      std::to_string(kMaxRequests) +
                                      ") across its cursor enumeration without exhausting it; refusing to scan "
                                      "a partial split enumeration");
        }
        auto cursor = std::move(pending_cursors.back());
        pending_cursors.pop_back();

        auto request = BuildTableFunctionPlanRequest(bind_call_bytes, bind_opaque_data, projection_ids,
                                                      pushdown_filters, cursor);
        auto request_bytes = wire::encode_ipc(request);
        auto params = gen::BuildTableFunctionPlanParams(std::vector<uint8_t>(request_bytes.begin(), request_bytes.end()));
        auto response = connection.CallUnary("table_function_plan", params);
        auto result_batch = wire::get_ipc(response.batch, "result");
        if (!result_batch) continue;
        ++requests;

        auto page = ParsePlanPage(result_batch);
        // Plan-level facts come from the FIRST response only, keyed on
        // request count rather than "first non-empty" - a leading
        // response may legally carry zero splits and a cursor (a worker
        // still enumerating) - matching vgi's own ParseVgiScanPlanPage.
        if (first_response) {
            plan.init_opaque_data = page.init_opaque_data;
            first_response = false;
        }
        for (auto& token : page.tokens) plan.split_tokens.push_back(std::move(token));

        if (plan.split_tokens.size() > kMaxSplits) {
            throw std::runtime_error("table_function_plan returned more than " + std::to_string(kMaxSplits) +
                                      " splits for one scan; refusing to buffer an unbounded split vector");
        }
        for (auto& next : page.next_cursors) pending_cursors.push_back(std::move(next));
    }
    return plan;
}

}  // namespace vgi_sqlite
