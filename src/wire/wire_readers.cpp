// © Copyright 2026 Query Farm LLC - https://query.farm
#include "wire/wire_readers.h"

#include <sstream>
#include <stdexcept>

#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace vgi_sqlite::wire {
namespace {

[[noreturn]] void fail(const std::string& what) { throw std::runtime_error(what); }

template <typename T>
T unwrap(arrow::Result<T> result, const std::string& context) {
    if (!result.ok()) fail(context + ": " + result.status().ToString());
    return std::move(result).ValueUnsafe();
}

void check_ok(const arrow::Status& status, const std::string& context) {
    if (!status.ok()) fail(context + ": " + status.ToString());
}

template <typename ArrayType>
std::shared_ptr<ArrayType> typed_column(const std::shared_ptr<arrow::RecordBatch>& batch,
                                         const std::string& field, const char* expected) {
    auto arr = column(batch, field);
    auto typed = std::dynamic_pointer_cast<ArrayType>(arr);
    if (!typed) {
        fail("field '" + field + "' is " + arr->type()->ToString() + ", expected " + expected);
    }
    if (typed->length() < 1) fail("field '" + field + "' batch has no rows");
    return typed;
}

// A field may be absent, not just null: the protocol adds nullable columns
// additively, so a peer built against an older protocol surface simply does
// not send/return them. Treating absence as an error would make every
// response from such a peer fail, which is exactly what the additive rule
// exists to prevent.
bool has_column(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field) {
    return batch && batch->GetColumnByName(field) != nullptr;
}

}  // namespace

std::shared_ptr<arrow::Array> column(const std::shared_ptr<arrow::RecordBatch>& batch,
                                      const std::string& field) {
    if (!batch) fail("field '" + field + "' requested from a null batch");
    auto arr = batch->GetColumnByName(field);
    if (!arr) {
        std::ostringstream have;
        for (int i = 0; i < batch->num_columns(); ++i) {
            if (i) have << ", ";
            have << batch->schema()->field(i)->name();
        }
        fail("no field '" + field + "'; batch carries [" + have.str() + "]");
    }
    return arr;
}

std::string get_string(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field) {
    auto arr = typed_column<arrow::StringArray>(batch, field, "string");
    if (arr->IsNull(0)) fail("field '" + field + "' is null but not optional");
    return arr->GetString(0);
}

std::optional<std::string> get_optional_string(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto arr = typed_column<arrow::StringArray>(batch, field, "string");
    if (arr->IsNull(0)) return std::nullopt;
    return arr->GetString(0);
}

std::string get_binary(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field) {
    auto arr = typed_column<arrow::BinaryArray>(batch, field, "binary");
    if (arr->IsNull(0)) fail("field '" + field + "' is null but not optional");
    return arr->GetString(0);
}

std::optional<std::string> get_optional_binary(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    // binary and large_binary alike: the protocol picks the wide form for
    // fields that can exceed 2 GiB, and they're the same value to every
    // caller here.
    auto arr = column(batch, field);
    if (auto wide = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(arr)) {
        return wide->IsNull(0) ? std::nullopt : std::optional<std::string>(wide->GetString(0));
    }
    auto narrow = typed_column<arrow::BinaryArray>(batch, field, "binary or large_binary");
    if (narrow->IsNull(0)) return std::nullopt;
    return narrow->GetString(0);
}

bool get_bool(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field) {
    auto arr = typed_column<arrow::BooleanArray>(batch, field, "boolean");
    if (arr->IsNull(0)) fail("field '" + field + "' is null but not optional");
    return arr->Value(0);
}

std::optional<bool> get_optional_bool(const std::shared_ptr<arrow::RecordBatch>& batch,
                                       const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto arr = typed_column<arrow::BooleanArray>(batch, field, "boolean");
    if (arr->IsNull(0)) return std::nullopt;
    return arr->Value(0);
}

int64_t get_int64(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field) {
    auto arr = typed_column<arrow::Int64Array>(batch, field, "int64");
    if (arr->IsNull(0)) fail("field '" + field + "' is null but not optional");
    return arr->Value(0);
}

std::optional<int64_t> get_optional_int64(const std::shared_ptr<arrow::RecordBatch>& batch,
                                           const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto arr = typed_column<arrow::Int64Array>(batch, field, "int64");
    if (arr->IsNull(0)) return std::nullopt;
    return arr->Value(0);
}

std::string get_enum(const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& field) {
    auto arr = typed_column<arrow::DictionaryArray>(batch, field, "dictionary");
    if (arr->IsNull(0)) fail("field '" + field + "' is null but not optional");
    auto values = std::dynamic_pointer_cast<arrow::StringArray>(arr->dictionary());
    if (!values) fail("field '" + field + "' is a dictionary of non-strings");
    const auto* indices = dynamic_cast<const arrow::Int16Array*>(arr->indices().get());
    if (!indices) fail("field '" + field + "' has non-int16 dictionary indices");
    const auto index = indices->Value(0);
    if (index < 0 || index >= values->length()) {
        fail("field '" + field + "' dictionary index " + std::to_string(index) + " is out of range");
    }
    return values->GetString(index);
}

std::optional<std::string> get_optional_enum(const std::shared_ptr<arrow::RecordBatch>& batch,
                                              const std::string& field) {
    if (!has_column(batch, field)) return std::nullopt;
    auto arr = typed_column<arrow::DictionaryArray>(batch, field, "dictionary");
    if (arr->IsNull(0)) return std::nullopt;
    auto values = std::dynamic_pointer_cast<arrow::StringArray>(arr->dictionary());
    if (!values) fail("field '" + field + "' is a dictionary of non-strings");
    const auto* indices = dynamic_cast<const arrow::Int16Array*>(arr->indices().get());
    if (!indices) fail("field '" + field + "' has non-int16 dictionary indices");
    const auto index = indices->Value(0);
    if (index < 0 || index >= values->length()) {
        fail("field '" + field + "' dictionary index " + std::to_string(index) + " is out of range");
    }
    return values->GetString(index);
}

std::shared_ptr<arrow::RecordBatch> decode_ipc(const std::string& bytes) {
    if (bytes.empty()) return nullptr;
    auto buffer = arrow::Buffer::FromString(bytes);
    auto source = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader =
        unwrap(arrow::ipc::RecordBatchStreamReader::Open(source), "reading an embedded IPC value");
    std::shared_ptr<arrow::RecordBatch> batch;
    check_ok(reader->ReadNext(&batch), "reading an embedded IPC batch");
    return batch;
}

std::vector<std::string> get_binary_list(const std::shared_ptr<arrow::RecordBatch>& batch,
                                          const std::string& field) {
    std::vector<std::string> items;
    if (!has_column(batch, field)) return items;
    auto list = std::dynamic_pointer_cast<arrow::ListArray>(column(batch, field));
    if (!list) fail("field '" + field + "' is not a list");
    if (list->length() == 0 || list->IsNull(0)) return items;

    const int64_t begin = list->value_offset(0);
    const int64_t end = list->value_offset(1);
    if (auto wide = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(list->values())) {
        for (int64_t i = begin; i < end; ++i) {
            if (!wide->IsNull(i)) items.push_back(wide->GetString(i));
        }
        return items;
    }
    auto narrow = std::dynamic_pointer_cast<arrow::BinaryArray>(list->values());
    if (!narrow) fail("field '" + field + "' is not a list of binary");
    for (int64_t i = begin; i < end; ++i) {
        if (!narrow->IsNull(i)) items.push_back(narrow->GetString(i));
    }
    return items;
}

std::vector<int64_t> get_int64_list(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::string& field) {
    std::vector<int64_t> items;
    if (!has_column(batch, field)) return items;
    auto list = std::dynamic_pointer_cast<arrow::ListArray>(column(batch, field));
    if (!list) fail("field '" + field + "' is not a list");
    if (list->length() == 0 || list->IsNull(0)) return items;
    auto values = std::dynamic_pointer_cast<arrow::Int64Array>(list->values());
    if (!values) fail("field '" + field + "' is not a list of int64");
    for (int64_t i = list->value_offset(0); i < list->value_offset(1); ++i) {
        if (!values->IsNull(i)) items.push_back(values->Value(i));
    }
    return items;
}

std::shared_ptr<arrow::RecordBatch> get_ipc(const std::shared_ptr<arrow::RecordBatch>& batch,
                                             const std::string& field) {
    auto bytes = get_optional_binary(batch, field);
    if (!bytes) return nullptr;
    return decode_ipc(*bytes);
}

std::string encode_ipc(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (auto status = batch->Validate(); !status.ok()) {
        throw std::runtime_error("encoding an invalid IPC batch: " + status.ToString());
    }
    auto sink = unwrap(arrow::io::BufferOutputStream::Create(), "allocating an IPC sink");
    auto writer =
        unwrap(arrow::ipc::MakeStreamWriter(sink, batch->schema()), "opening an IPC writer");
    check_ok(writer->WriteRecordBatch(*batch), "writing an embedded IPC batch");
    check_ok(writer->Close(), "closing an embedded IPC stream");
    auto buffer = unwrap(sink->Finish(), "finishing an IPC sink");
    return buffer->ToString();
}

std::shared_ptr<arrow::Schema> decode_schema(const std::string& bytes) {
    if (bytes.empty()) return nullptr;
    auto buffer = arrow::Buffer::FromString(bytes);
    auto source = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader =
        unwrap(arrow::ipc::RecordBatchStreamReader::Open(source), "reading an embedded IPC schema");
    return reader->schema();
}

std::shared_ptr<arrow::Schema> get_schema(const std::shared_ptr<arrow::RecordBatch>& batch,
                                           const std::string& field) {
    auto bytes = get_optional_binary(batch, field);
    if (!bytes) return nullptr;
    return decode_schema(*bytes);
}

std::string encode_schema(const std::shared_ptr<arrow::Schema>& schema) {
    auto sink = unwrap(arrow::io::BufferOutputStream::Create(), "allocating a schema sink");
    auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, schema), "opening a schema writer");
    check_ok(writer->Close(), "closing a schema-only IPC stream");
    auto buffer = unwrap(sink->Finish(), "finishing a schema sink");
    return buffer->ToString();
}

}  // namespace vgi_sqlite::wire
