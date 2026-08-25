// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Reading VGI RPC response batches, and the IPC-embedded-value codec every
// dataclass-as-binary field on the wire uses.
//
// Mirrors vgi-c++'s (the worker SDK's) private src/wire.h "reading params"
// half exactly, in spirit and in most cases byte-for-byte logic — the
// accessors are generic column readers, direction-agnostic in what they do
// (vgi-c++ uses them to read a worker's inbound request batch; we use the
// identical logic to read a response batch back from a worker). Not a
// shared link target (that header is private to vgi-c++, see the plan file)
// so this is a from-scratch port, not a copy.
//
// The shape of the wire, same as there:
//   * A result batch is one row of the method's generated result schema.
//   * Every non-void RPC answers a one-column outer envelope,
//     {result: binary}: for a Result-kind method those bytes are a
//     self-describing IPC stream of that one-row batch (decode_ipc); for a
//     Binary-kind method the bytes are the payload verbatim.
//   * A field whose type is a dataclass travels as `binary` holding a
//     complete IPC stream of a one-row batch (get_ipc/decode_ipc).
//   * A response that is a *list* of items carries them as `list<binary>`,
//     each element itself an IPC stream (get_binary_list, then decode_ipc
//     each element).
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type.h>

namespace vgi_sqlite::wire {

// Column lookup that says which field was missing and what was there
// instead.
std::shared_ptr<arrow::Array> column(const std::shared_ptr<arrow::RecordBatch>& batch,
                                      const std::string& field);

std::string get_string(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field);
std::optional<std::string> get_optional_string(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                const std::string& field);

std::string get_binary(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field);
std::optional<std::string> get_optional_binary(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                const std::string& field);

bool get_bool(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field);
std::optional<bool> get_optional_bool(const std::shared_ptr<arrow::RecordBatch>& batch,
                                       const std::string& field);
int64_t get_int64(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field);
std::optional<int64_t> get_optional_int64(const std::shared_ptr<arrow::RecordBatch>& batch,
                                           const std::string& field);

// A dictionary<int16, utf8> field, read back as its string value.
std::string get_enum(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field);
std::optional<std::string> get_optional_enum(const std::shared_ptr<arrow::RecordBatch>& batch,
                                              const std::string& field);

// ── IPC-embedded values ───────────────────────────────────────────────────

// Decode a self-describing IPC stream into its single batch. Returns null
// for a stream carrying a schema but no batches (how an absent optional
// dataclass, or a zero-item list element, travels).
std::shared_ptr<arrow::RecordBatch> decode_ipc(const std::string& bytes);

// A `list<binary>` field, as its raw element bytes (each element still an
// encoded IPC stream the caller decodes separately, e.g. via decode_ipc).
std::vector<std::string> get_binary_list(const std::shared_ptr<arrow::RecordBatch>& batch,
                                          const std::string& field);

std::vector<int64_t> get_int64_list(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::string& field);

// Read a dataclass field: the named binary column, decoded.
std::shared_ptr<arrow::RecordBatch> get_ipc(const std::shared_ptr<arrow::RecordBatch>& batch,
                                             const std::string& field);

std::string encode_ipc(const std::shared_ptr<arrow::RecordBatch>& batch);

// An IPC stream carrying a schema and no batches (for schema-valued fields
// like output_schema, whose field *metadata* is the payload).
std::string encode_schema(const std::shared_ptr<arrow::Schema>& schema);
std::shared_ptr<arrow::Schema> decode_schema(const std::string& bytes);
std::shared_ptr<arrow::Schema> get_schema(const std::shared_ptr<arrow::RecordBatch>& batch,
                                           const std::string& field);

}  // namespace vgi_sqlite::wire
