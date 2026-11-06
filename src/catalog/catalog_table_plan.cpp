// © Copyright 2026 Query Farm LLC - https://query.farm
#include "catalog/catalog_table_plan.h"

#include <stdexcept>

#include "generated/vgi_request_builders.hpp"
#include "wire/wire_readers.h"

namespace vgi_sqlite {
namespace {
namespace gen = ::vgi::generated;

}  // namespace

nano::BatchPtr BuildTableFunctionPlanRequest(
    const std::vector<uint8_t>& bind_call_bytes, const std::vector<uint8_t>& bind_opaque_data,
    const std::vector<int64_t>& projection_ids, const std::optional<std::string>& pushdown_filters,
    const std::vector<uint8_t>& cursor) {
    auto request = nano::Batch::Named("TableFunctionPlanRequest");
    request->SetBytes("bind_call", bind_call_bytes);
    request->SetBytes("bind_opaque_data", bind_opaque_data.empty()
                                                  ? std::optional<std::vector<uint8_t>>{}
                                                  : std::optional<std::vector<uint8_t>>(bind_opaque_data));
    request->SetInt64List("projection_ids", projection_ids.empty()
                                                   ? std::optional<std::vector<int64_t>>{}
                                                   : std::optional<std::vector<int64_t>>(projection_ids));
    request->SetBytes("pushdown_filters", pushdown_filters);
    request->SetNull("join_keys");
    request->SetNull("row_limit");
    request->SetNull("target_split_bytes");
    request->SetNull("min_splits");
    request->SetNull("max_splits_per_response");
    request->SetBytes("cursor", cursor.empty() ? std::optional<std::vector<uint8_t>>{}
                                                : std::optional<std::vector<uint8_t>>(cursor));
    request->SetNull("refined_filters");
    request->SetBool("filters_complete", true);
    request->SetNull("start_position");
    request->SetNull("end_position");
    request->SetNull("order_by_column_name");
    request->SetNull("order_by_direction");
    request->SetNull("order_by_null_order");
    request->SetNull("order_by_limit");
    request->SetNull("tablesample_percentage");
    request->SetNull("tablesample_seed");
    request->FinishRow();
    return request;
}

namespace {

// One page's worth of a plan response - mirrors vgi's own
// ParseVgiScanPlanPage (vgi_catalog_api.cpp) closely, trimmed to what a
// sequential single-reader client needs. Protocol 3.1's exact/range split
// contract is retained long enough to validate it; optimizer-only locations,
// sort_order, cache_max_age_seconds, and estimates remain intentionally
// unconsumed because SQLite has no scheduler or physical-property planner.
struct PlanPage {
    struct Split {
        std::vector<uint8_t> token;
        std::optional<std::string> key_value;
        std::optional<int64_t> range_index;
        std::optional<std::string> column_bounds;
    };
    std::vector<Split> splits;
    // Every cursor this page returned, not just the first - see
    // PlanTableFunctionSplits's comment on why all of them must be
    // followed. Empty = this cursor's enumeration is exhausted.
    std::vector<std::vector<uint8_t>> next_cursors;
    std::vector<uint8_t> init_opaque_data;
    std::optional<std::string> key_grouping;
    std::optional<std::string> range_partitioning;
};

PlanPage ParsePlanPage(const nano::BatchPtr& batch) {
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
        PlanPage::Split split;
        split.token.assign(token->begin(), token->end());
        split.key_value = wire::get_optional_binary(split_batch, "key_value");
        split.range_index = wire::get_optional_int64(split_batch, "range_index");
        split.column_bounds = wire::get_optional_binary(split_batch, "column_bounds");
        page.splits.push_back(std::move(split));
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
    page.key_grouping = wire::get_optional_binary(batch, "key_grouping");
    page.range_partitioning = wire::get_optional_binary(batch, "range_partitioning");
    return page;
}

struct TransformKey {
    const ArrowSchema* identity_field = nullptr;
};

TransformKey ParseTransform(const std::string& bytes, const nano::SchemaPtr& output_schema,
                            const std::string& context) {
    auto transform = wire::decode_ipc(bytes);
    if (!transform || transform->num_rows() != 1) {
        throw std::runtime_error(context + " transform must contain exactly one row");
    }
    const auto id = wire::get_string(transform, "id");
    const auto inputs = wire::get_binary_list(transform, "inputs");
    const auto parameters = wire::get_optional_binary(transform, "parameters");
    for (const auto& input : inputs) {
        auto path = wire::decode_ipc(input);
        if (!path || path->num_rows() != 1 || wire::get_string_list(path, "components").empty()) {
            throw std::runtime_error(context + " transform input must be a non-empty FieldPath");
        }
    }

    TransformKey result;
    if (id != "vgi.identity/1" || parameters || inputs.size() != 1) return result;
    auto path = wire::decode_ipc(inputs.front());
    const auto components = wire::get_string_list(path, "components");
    if (components.size() != 1) return result;
    result.identity_field = output_schema ? output_schema->field(components.front()) : nullptr;
    if (!result.identity_field) {
        throw std::runtime_error(context + " identity field '" + components.front() + "' is not in the output schema");
    }
    return result;
}

std::vector<TransformKey> ParseKeyGrouping(const std::string& bytes,
                                           const nano::SchemaPtr& output_schema) {
    auto grouping = wire::decode_ipc(bytes);
    if (!grouping || grouping->num_rows() != 1) {
        throw std::runtime_error("key_grouping must contain exactly one row");
    }
    const auto keys = wire::get_binary_list(grouping, "keys");
    if (keys.empty()) throw std::runtime_error("key_grouping must contain at least one key");
    std::vector<TransformKey> parsed;
    parsed.reserve(keys.size());
    for (const auto& key : keys) parsed.push_back(ParseTransform(key, output_schema, "key_grouping"));
    return parsed;
}

struct ParsedRangePartitioning {
    std::vector<TransformKey> keys;
    int64_t split_point_count = 0;
};

ParsedRangePartitioning ParseRangePartitioning(const std::string& bytes,
                                               const nano::SchemaPtr& output_schema) {
    auto range = wire::decode_ipc(bytes);
    if (!range || range->num_rows() != 1) {
        throw std::runtime_error("range_partitioning must contain exactly one row");
    }
    const auto encoded_keys = wire::get_binary_list(range, "keys");
    if (encoded_keys.empty()) throw std::runtime_error("range_partitioning must contain at least one key");

    ParsedRangePartitioning parsed;
    parsed.keys.reserve(encoded_keys.size());
    for (const auto& encoded_key : encoded_keys) {
        auto key = wire::decode_ipc(encoded_key);
        if (!key || key->num_rows() != 1) {
            throw std::runtime_error("range_partitioning key must contain exactly one row");
        }
        // Reading these fields validates the comparator, including their enum wire
        // types.
        (void)wire::get_enum(key, "direction");
        (void)wire::get_enum(key, "null_order");
        parsed.keys.push_back(ParseTransform(wire::get_binary(key, "transform"), output_schema, "range_partitioning"));
    }

    auto split_points = wire::get_ipc(range, "split_points");
    if (!split_points) throw std::runtime_error("range_partitioning split_points is not an Arrow batch");
    if (split_points->num_columns() != static_cast<int>(parsed.keys.size())) {
        throw std::runtime_error("range_partitioning split_points has " + std::to_string(split_points->num_columns()) +
                                 " columns; expected " + std::to_string(parsed.keys.size()));
    }
    for (int i = 0; i < split_points->num_columns(); ++i) {
        const auto* expected = parsed.keys[static_cast<size_t>(i)].identity_field;
        const auto* actual = split_points->raw().schema.children[i];
        if (expected && nano::TypeName(*actual) != nano::TypeName(*expected)) {
            throw std::runtime_error("range_partitioning split point column '" +
                                     std::string(expected->name ? expected->name : "") + "' has type " +
                                     nano::TypeName(*actual) + "; expected " + nano::TypeName(*expected));
        }
    }
    parsed.split_point_count = split_points->num_rows();
    return parsed;
}

void ValidateSplitMetadata(const std::vector<PlanPage::Split>& splits, const std::optional<std::string>& key_grouping,
                           const std::optional<std::string>& range_partitioning,
                           const nano::SchemaPtr& output_schema) {
    std::optional<std::vector<TransformKey>> grouping;
    if (key_grouping) grouping = ParseKeyGrouping(*key_grouping, output_schema);
    std::optional<ParsedRangePartitioning> ranges;
    if (range_partitioning) ranges = ParseRangePartitioning(*range_partitioning, output_schema);

    for (size_t split_index = 0; split_index < splits.size(); ++split_index) {
        const auto& split = splits[split_index];
        if (grouping) {
            if (!split.key_value) {
                throw std::runtime_error("declared an exact key grouping but split " + std::to_string(split_index) +
                                         " has no key_value");
            }
            auto key = wire::decode_ipc(*split.key_value);
            if (!key || key->num_rows() != 1) {
                const auto rows = key ? key->num_rows() : 0;
                throw std::runtime_error("split " + std::to_string(split_index) +
                                         " key_value must contain exactly one row (got " + std::to_string(rows) + ")");
            }
            if (key->num_columns() != static_cast<int>(grouping->size())) {
                throw std::runtime_error("split " + std::to_string(split_index) + " key_value has " +
                                         std::to_string(key->num_columns()) + " columns; expected " +
                                         std::to_string(grouping->size()));
            }
            for (int i = 0; i < key->num_columns(); ++i) {
                const auto* expected = (*grouping)[static_cast<size_t>(i)].identity_field;
                const auto* actual = key->raw().schema.children[i];
                if (expected && nano::TypeName(*actual) != nano::TypeName(*expected)) {
                    throw std::runtime_error("split " + std::to_string(split_index) + " key_value column '" +
                                             std::string(expected->name ? expected->name : "") + "' has type " +
                                             nano::TypeName(*actual) + "; expected " + nano::TypeName(*expected));
                }
            }
        } else if (split.key_value) {
            throw std::runtime_error("split " + std::to_string(split_index) +
                                     " has key_value but the plan has no key_grouping");
        }

        if (ranges) {
            if (!split.range_index) {
                throw std::runtime_error("declared range partitioning but split " + std::to_string(split_index) +
                                         " has no range_index");
            }
            if (*split.range_index < 0 || *split.range_index > ranges->split_point_count) {
                throw std::runtime_error("split " + std::to_string(split_index) + " range_index " +
                                         std::to_string(*split.range_index) + " is outside [0, " +
                                         std::to_string(ranges->split_point_count) + "]");
            }
        } else if (split.range_index) {
            throw std::runtime_error("split " + std::to_string(split_index) +
                                     " has range_index but the plan has no range_partitioning");
        }

        if (split.column_bounds) {
            auto bounds = wire::decode_ipc(*split.column_bounds);
            if (!bounds || bounds->num_rows() != 2) {
                const auto rows = bounds ? bounds->num_rows() : 0;
                throw std::runtime_error("split " + std::to_string(split_index) +
                                         " column_bounds must contain exactly two rows (got " + std::to_string(rows) +
                                         ")");
            }
        }
    }
}

}  // namespace

TableFunctionPlan PlanTableFunctionSplits(VgiConnection& connection, const std::vector<uint8_t>& bind_call_bytes,
                                          const std::vector<uint8_t>& bind_opaque_data,
                                          const nano::SchemaPtr& output_schema,
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
    std::vector<PlanPage::Split> splits;
    std::optional<std::string> key_grouping;
    std::optional<std::string> range_partitioning;
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
            key_grouping = page.key_grouping;
            range_partitioning = page.range_partitioning;
            first_response = false;
        }
        for (auto& split : page.splits) splits.push_back(std::move(split));

        if (splits.size() > kMaxSplits) {
            throw std::runtime_error("table_function_plan returned more than " + std::to_string(kMaxSplits) +
                                      " splits for one scan; refusing to buffer an unbounded split vector");
        }
        for (auto& next : page.next_cursors) pending_cursors.push_back(std::move(next));
    }
    ValidateSplitMetadata(splits, key_grouping, range_partitioning, output_schema);
    plan.split_tokens.reserve(splits.size());
    for (auto& split : splits) plan.split_tokens.push_back(std::move(split.token));
    return plan;
}

}  // namespace vgi_sqlite
