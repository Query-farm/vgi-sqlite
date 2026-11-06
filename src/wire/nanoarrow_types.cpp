// © Copyright 2026 Query Farm LLC - https://query.farm
#include "wire/nanoarrow_types.h"

#include <cerrno>
#include <cmath>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>

#include <vgi_rpc/ipc.h>
#include <vgi_rpc/stream.h>

namespace vgi_sqlite::nano {
namespace {

std::string ErrorText(int code, const ArrowError& error) {
    if (error.message[0]) return error.message;
    const char* text = std::strerror(code);
    return text ? text : "unknown nanoarrow error";
}

void MoveSchema(ArrowSchema* source, ArrowSchema* target) {
    *target = *source;
    std::memset(source, 0, sizeof(*source));
}

void MoveBatch(VgiBatch* source, VgiBatch* target) {
    *target = *source;
    std::memset(source, 0, sizeof(*source));
}

std::string MetadataBytes(const std::vector<std::pair<std::string, std::string>>& values) {
    ArrowBuffer metadata;
    ArrowBufferInit(&metadata);
    Check(ArrowMetadataBuilderInit(&metadata, nullptr), "initialize field metadata");
    for (const auto& [key, value] : values) {
        Check(ArrowMetadataBuilderAppend(&metadata, ArrowCharView(key.c_str()),
                                         ArrowCharView(value.c_str())),
              "append field metadata");
    }
    std::string result(reinterpret_cast<const char*>(metadata.data),
                       static_cast<size_t>(metadata.size_bytes));
    ArrowBufferReset(&metadata);
    return result;
}

ArrowArray* MutableColumn(VgiBatch& batch, const std::string& name) {
    auto* column = VgiBatchColumn(&batch, name.c_str());
    if (!column) throw std::runtime_error("no field '" + name + "' in nanoarrow batch");
    return column;
}

const ArrowSchema* ColumnSchema(const VgiBatch& batch, int64_t index) {
    if (index < 0 || index >= batch.schema.n_children) return nullptr;
    return batch.schema.children[index];
}

template <typename T>
bool InRange(int64_t value) {
    return value >= static_cast<int64_t>(std::numeric_limits<T>::min()) &&
           value <= static_cast<int64_t>(std::numeric_limits<T>::max());
}

void AppendScalarValue(ArrowArray* output, const ArrowSchema& target, const Scalar& scalar) {
    if (scalar.kind == Scalar::Kind::Null) {
        Check(ArrowArrayAppendNull(output, 1), "append null scalar");
        return;
    }
    ArrowError error;
    ArrowErrorInit(&error);
    ArrowSchemaView type;
    Check(ArrowSchemaViewInit(&type, &target, &error), error, "inspect scalar target type");

    if (output->dictionary) {
        if (scalar.kind != Scalar::Kind::String)
            throw std::runtime_error("dictionary scalar requires a string value");
        Check(VgiArrayAppendString(output,
                                   ArrowCharView(std::get<std::string>(scalar.value).c_str())),
              "append dictionary scalar");
        return;
    }

    auto signed_value = [&]() -> int64_t {
        if (scalar.kind == Scalar::Kind::Signed) return std::get<int64_t>(scalar.value);
        if (scalar.kind == Scalar::Kind::Unsigned) {
            auto value = std::get<uint64_t>(scalar.value);
            if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
                throw std::runtime_error("unsigned scalar exceeds int64 range");
            return static_cast<int64_t>(value);
        }
        if (scalar.kind == Scalar::Kind::Bool) return std::get<bool>(scalar.value) ? 1 : 0;
        if (scalar.kind == Scalar::Kind::Double) {
            auto value = std::get<double>(scalar.value);
            if (!std::isfinite(value) || std::trunc(value) != value ||
                value < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
                value > static_cast<double>(std::numeric_limits<int64_t>::max()))
                throw std::runtime_error("lossy floating-point to integer scalar conversion");
            return static_cast<int64_t>(value);
        }
        throw std::runtime_error("scalar is not numeric");
    };
    auto unsigned_value = [&]() -> uint64_t {
        if (scalar.kind == Scalar::Kind::Unsigned) return std::get<uint64_t>(scalar.value);
        auto value = signed_value();
        if (value < 0) throw std::runtime_error("negative scalar cannot convert to unsigned");
        return static_cast<uint64_t>(value);
    };
    auto real_value = [&]() -> double {
        if (scalar.kind == Scalar::Kind::Double) return std::get<double>(scalar.value);
        if (scalar.kind == Scalar::Kind::Unsigned)
            return static_cast<double>(std::get<uint64_t>(scalar.value));
        if (scalar.kind == Scalar::Kind::Bool) return std::get<bool>(scalar.value) ? 1.0 : 0.0;
        if (scalar.kind == Scalar::Kind::Signed)
            return static_cast<double>(std::get<int64_t>(scalar.value));
        throw std::runtime_error("scalar is not numeric");
    };

    switch (type.type) {
        case NANOARROW_TYPE_BOOL:
            Check(ArrowArrayAppendInt(output, signed_value() != 0), "append bool scalar");
            return;
        case NANOARROW_TYPE_INT8: {
            auto value = signed_value();
            if (!InRange<int8_t>(value)) throw std::runtime_error("int8 scalar out of range");
            Check(ArrowArrayAppendInt(output, value), "append int8 scalar");
            return;
        }
        case NANOARROW_TYPE_INT16: {
            auto value = signed_value();
            if (!InRange<int16_t>(value)) throw std::runtime_error("int16 scalar out of range");
            Check(ArrowArrayAppendInt(output, value), "append int16 scalar");
            return;
        }
        case NANOARROW_TYPE_INT32: {
            auto value = signed_value();
            if (!InRange<int32_t>(value)) throw std::runtime_error("int32 scalar out of range");
            Check(ArrowArrayAppendInt(output, value), "append int32 scalar");
            return;
        }
        case NANOARROW_TYPE_INT64:
            Check(ArrowArrayAppendInt(output, signed_value()), "append int64 scalar");
            return;
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64:
            Check(ArrowArrayAppendUInt(output, unsigned_value()), "append unsigned scalar");
            return;
        case NANOARROW_TYPE_HALF_FLOAT:
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE:
            Check(ArrowArrayAppendDouble(output, real_value()), "append floating scalar");
            return;
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING: {
            if (scalar.kind != Scalar::Kind::String)
                throw std::runtime_error("string scalar requires a text value");
            const auto& value = std::get<std::string>(scalar.value);
            Check(ArrowArrayAppendString(output, {value.data(), static_cast<int64_t>(value.size())}),
                  "append string scalar");
            return;
        }
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY: {
            if (scalar.kind != Scalar::Kind::Binary)
                throw std::runtime_error("binary scalar requires a blob value");
            const auto& value = std::get<std::vector<uint8_t>>(scalar.value);
            Check(ArrowArrayAppendBytes(output, {{value.data()}, static_cast<int64_t>(value.size())}),
                  "append binary scalar");
            return;
        }
        default:
            throw std::runtime_error("unsupported SQLite-to-Arrow scalar target " + TypeName(target));
    }
}

}  // namespace

[[noreturn]] void ThrowError(int code, const ArrowError& error, const std::string& operation) {
    throw std::runtime_error(operation + ": " + ErrorText(code, error));
}

void Check(int code, const std::string& operation) {
    if (code != NANOARROW_OK) {
        ArrowError error;
        ArrowErrorInit(&error);
        ThrowError(code, error, operation);
    }
}

void Check(int code, const ArrowError& error, const std::string& operation) {
    if (code != NANOARROW_OK) ThrowError(code, error, operation);
}

Schema::Schema() { ArrowSchemaInit(&schema_); }
Schema::~Schema() {
    if (schema_.release) ArrowSchemaRelease(&schema_);
}
Schema::Schema(Schema&& other) noexcept { MoveSchema(&other.schema_, &schema_); }
Schema& Schema::operator=(Schema&& other) noexcept {
    if (this != &other) {
        if (schema_.release) ArrowSchemaRelease(&schema_);
        MoveSchema(&other.schema_, &schema_);
    }
    return *this;
}
std::shared_ptr<Schema> Schema::Adopt(ArrowSchema* schema) {
    auto result = std::make_shared<Schema>();
    if (result->schema_.release) ArrowSchemaRelease(&result->schema_);
    MoveSchema(schema, &result->schema_);
    return result;
}
std::shared_ptr<Schema> Schema::Copy(const ArrowSchema& schema) {
    auto result = std::make_shared<Schema>();
    if (result->schema_.release) ArrowSchemaRelease(&result->schema_);
    Check(ArrowSchemaDeepCopy(&schema, &result->schema_), "copy Arrow schema");
    return result;
}
std::shared_ptr<Schema> Schema::Named(const char* name) {
    ArrowSchema schema{};
    ArrowError error;
    ArrowErrorInit(&error);
    Check(VgiSchemaInit(name, &schema, &error), error, std::string("initialize schema ") + name);
    return Adopt(&schema);
}
const ArrowSchema* Schema::field(int64_t index) const {
    return index < 0 || index >= schema_.n_children ? nullptr : schema_.children[index];
}
const ArrowSchema* Schema::field(const std::string& name) const {
    auto index = VgiFieldIndex(&schema_, name.c_str());
    return field(index);
}
std::shared_ptr<Schema> Schema::Select(const std::vector<int>& indices) const {
    std::vector<SchemaPtr> fields;
    fields.reserve(indices.size());
    for (int index : indices) {
        const auto* source = field(index);
        if (!source) throw std::runtime_error("schema selection index out of range");
        fields.push_back(Copy(*source));
    }
    return MakeSchema(fields);
}
bool Schema::Equals(const Schema& other, bool metadata) const {
    auto equal = [&](auto&& self, const ArrowSchema& left, const ArrowSchema& right) -> bool {
        if (std::string(left.format ? left.format : "") != std::string(right.format ? right.format : "") ||
            std::string(left.name ? left.name : "") != std::string(right.name ? right.name : "") ||
            left.flags != right.flags || left.n_children != right.n_children ||
            static_cast<bool>(left.dictionary) != static_cast<bool>(right.dictionary))
            return false;
        if (metadata) {
            auto left_size = ArrowMetadataSizeOf(left.metadata);
            auto right_size = ArrowMetadataSizeOf(right.metadata);
            if (left_size != right_size ||
                (left_size && std::memcmp(left.metadata, right.metadata,
                                          static_cast<size_t>(left_size)) != 0))
                return false;
        }
        for (int64_t i = 0; i < left.n_children; ++i)
            if (!self(self, *left.children[i], *right.children[i])) return false;
        return !left.dictionary || self(self, *left.dictionary, *right.dictionary);
    };
    return equal(equal, schema_, other.schema_);
}
std::string Schema::ToString() const {
    std::ostringstream out;
    out << "struct<";
    for (int64_t i = 0; i < schema_.n_children; ++i) {
        if (i) out << ", ";
        out << (schema_.children[i]->name ? schema_.children[i]->name : "") << ": "
            << TypeName(*schema_.children[i]);
    }
    return out.str() + ">";
}
void Schema::SetMetadata(const std::vector<std::pair<std::string, std::string>>& metadata) {
    auto encoded = MetadataBytes(metadata);
    Check(ArrowSchemaSetMetadata(&schema_, encoded.data()), "set schema metadata");
}

SchemaPtr Type(enum ArrowType type) {
    auto result = std::make_shared<Schema>();
    Check(ArrowSchemaSetType(&result->raw(), type), "set Arrow type");
    return result;
}
SchemaPtr ListType(const SchemaPtr& value_type, bool large) {
    auto result = Type(large ? NANOARROW_TYPE_LARGE_LIST : NANOARROW_TYPE_LIST);
    if (result->raw().children[0]->release) ArrowSchemaRelease(result->raw().children[0]);
    Check(ArrowSchemaDeepCopy(&value_type->raw(), result->raw().children[0]), "copy list child type");
    Check(ArrowSchemaSetName(result->raw().children[0], "item"), "name list child");
    return result;
}
SchemaPtr StructType(const std::vector<SchemaPtr>& fields) { return MakeSchema(fields); }
SchemaPtr DictionaryType(enum ArrowType index_type, const SchemaPtr& value_type) {
    auto result = Type(index_type);
    Check(ArrowSchemaAllocateDictionary(&result->raw()), "allocate dictionary schema");
    Check(ArrowSchemaDeepCopy(&value_type->raw(), result->raw().dictionary),
          "copy dictionary value schema");
    return result;
}
SchemaPtr Field(std::string name, const SchemaPtr& type, bool nullable,
                const std::vector<std::pair<std::string, std::string>>& metadata) {
    auto result = Schema::Copy(type->raw());
    Check(ArrowSchemaSetName(&result->raw(), name.c_str()), "set field name");
    if (nullable)
        result->raw().flags |= ARROW_FLAG_NULLABLE;
    else
        result->raw().flags &= ~ARROW_FLAG_NULLABLE;
    if (!metadata.empty()) {
        auto encoded = MetadataBytes(metadata);
        Check(ArrowSchemaSetMetadata(&result->raw(), encoded.data()), "set field metadata");
    }
    return result;
}
SchemaPtr MakeSchema(const std::vector<SchemaPtr>& fields) {
    ArrowSchema schema;
    ArrowSchemaInit(&schema);
    Check(ArrowSchemaSetTypeStruct(&schema, static_cast<int64_t>(fields.size())),
          "initialize struct schema");
    schema.flags &= ~ARROW_FLAG_NULLABLE;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (schema.children[i]->release) ArrowSchemaRelease(schema.children[i]);
        Check(ArrowSchemaDeepCopy(&fields[i]->raw(), schema.children[i]), "copy schema field");
    }
    return Schema::Adopt(&schema);
}
enum ArrowType TypeId(const ArrowSchema& schema) {
    ArrowSchemaView view;
    ArrowError error;
    ArrowErrorInit(&error);
    Check(ArrowSchemaViewInit(&view, &schema, &error), error, "inspect Arrow type");
    return view.type;
}
std::string TypeName(const ArrowSchema& schema) {
    ArrowSchemaView view;
    ArrowError error;
    ArrowErrorInit(&error);
    Check(ArrowSchemaViewInit(&view, &schema, &error), error, "inspect Arrow type");
    if (schema.dictionary) return "dictionary<" + TypeName(*schema.dictionary) + ">";
    return ArrowTypeString(view.type);
}
std::optional<std::string> MetadataValue(const ArrowSchema& schema, const std::string& key) {
    if (!schema.metadata) return std::nullopt;
    ArrowStringView value{nullptr, 0};
    if (ArrowMetadataGetValue(schema.metadata, ArrowCharView(key.c_str()), &value) != NANOARROW_OK ||
        !value.data)
        return std::nullopt;
    return std::string(value.data, static_cast<size_t>(value.size_bytes));
}

std::shared_ptr<Scalar> Scalar::Null(const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type; return out;
}
std::shared_ptr<Scalar> Scalar::Signed(int64_t value, const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type ? type : Type(NANOARROW_TYPE_INT64);
    out->kind = Kind::Signed; out->value = value; return out;
}
std::shared_ptr<Scalar> Scalar::Unsigned(uint64_t value, const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type ? type : Type(NANOARROW_TYPE_UINT64);
    out->kind = Kind::Unsigned; out->value = value; return out;
}
std::shared_ptr<Scalar> Scalar::Real(double value, const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type ? type : Type(NANOARROW_TYPE_DOUBLE);
    out->kind = Kind::Double; out->value = value; return out;
}
std::shared_ptr<Scalar> Scalar::Boolean(bool value, const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type ? type : Type(NANOARROW_TYPE_BOOL);
    out->kind = Kind::Bool; out->value = value; return out;
}
std::shared_ptr<Scalar> Scalar::String(std::string value, const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type ? type : Type(NANOARROW_TYPE_STRING);
    out->kind = Kind::String; out->value = std::move(value); return out;
}
std::shared_ptr<Scalar> Scalar::Binary(std::vector<uint8_t> value, const SchemaPtr& type) {
    auto out = std::make_shared<Scalar>(); out->type = type ? type : Type(NANOARROW_TYPE_BINARY);
    out->kind = Kind::Binary; out->value = std::move(value); return out;
}

bool Column::IsNull(int64_t row) const {
    return !view || row < 0 || row >= view->length || ArrowArrayViewIsNull(view, row);
}
enum ArrowType Column::type_id() const { return schema ? TypeId(*schema) : NANOARROW_TYPE_UNINITIALIZED; }
std::string Column::TypeString() const { return schema ? TypeName(*schema) : "invalid"; }
Column Column::child(int64_t index) const {
    if (!view || !schema || index < 0 || index >= view->n_children || index >= schema->n_children) return {};
    return {view->children[index], schema->children[index]};
}
Column Column::dictionary() const {
    return view && schema ? Column{view->dictionary, schema->dictionary} : Column{};
}
int64_t Column::Int(int64_t row) const { return ArrowArrayViewGetIntUnsafe(view, row); }
uint64_t Column::UInt(int64_t row) const { return ArrowArrayViewGetUIntUnsafe(view, row); }
double Column::Double(int64_t row) const { return ArrowArrayViewGetDoubleUnsafe(view, row); }
bool Column::Bool(int64_t row) const { return ArrowArrayViewGetIntUnsafe(view, row) != 0; }
std::string Column::String(int64_t row) const {
    auto value = VgiValueString(view, row);
    return value.data ? std::string(value.data, static_cast<size_t>(value.size_bytes)) : std::string();
}
std::vector<uint8_t> Column::Bytes(int64_t row) const {
    auto value = VgiValueString(view, row);
    const auto* begin = reinterpret_cast<const uint8_t*>(value.data);
    return value.data ? std::vector<uint8_t>(begin, begin + value.size_bytes) : std::vector<uint8_t>();
}
int64_t Column::Offset(int64_t row) const { return ArrowArrayViewListChildOffset(view, row); }

Batch::Batch() = default;
Batch::~Batch() { VgiBatchReset(&batch_); }
Batch::Batch(Batch&& other) noexcept { MoveBatch(&other.batch_, &batch_); }
Batch& Batch::operator=(Batch&& other) noexcept {
    if (this != &other) { VgiBatchReset(&batch_); MoveBatch(&other.batch_, &batch_); }
    return *this;
}
std::shared_ptr<Batch> Batch::Create(const SchemaPtr& schema) {
    auto out = std::make_shared<Batch>();
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiBatchInit(&out->batch_, &schema->raw(), &error), error, "initialize batch");
    return out;
}
std::shared_ptr<Batch> Batch::Named(const char* name) {
    auto out = std::make_shared<Batch>();
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiBatchInitNamed(&out->batch_, name, &error), error,
          std::string("initialize batch ") + name);
    return out;
}
std::shared_ptr<Batch> Batch::Decode(const void* data, int64_t size) {
    auto out = std::make_shared<Batch>();
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiBatchDecode(&out->batch_, {{data}, size}, &error), error, "decode IPC batch");
    return out;
}
std::shared_ptr<Batch> Batch::Adopt(VgiBatch* batch) {
    auto out = std::make_shared<Batch>();
    MoveBatch(batch, &out->batch_);
    ArrowError error;
    ArrowErrorInit(&error);
    Check(VgiBatchView(&out->batch_, &error), error, "initialize received batch view");
    return out;
}
std::shared_ptr<Batch> Batch::FromScalars(const SchemaPtr& schema,
                                          const std::vector<ScalarPtr>& values) {
    if (schema->num_fields() != static_cast<int64_t>(values.size()))
        throw std::runtime_error("batch scalar count does not match schema");
    auto out = Create(schema);
    for (int64_t i = 0; i < schema->num_fields(); ++i) out->AppendScalar(i, *values[i]);
    out->FinishRow();
    return out;
}
Column Batch::column(int64_t index) const {
    if (index < 0 || index >= batch_.view.n_children) return {};
    return {batch_.view.children[index], ColumnSchema(batch_, index)};
}
Column Batch::column(const std::string& name) const {
    return column(VgiFieldIndex(&batch_.schema, name.c_str()));
}
void Batch::SetNull(const std::string& name) { Check(VgiBatchSetNull(&batch_, name.c_str()), "set null field " + name); }
void Batch::SetString(const std::string& name, const std::optional<std::string>& value) {
    if (!value) return SetNull(name);
    Check(VgiBatchSetString(&batch_, name.c_str(), value->c_str()), "set string field " + name);
}
void Batch::SetBytes(const std::string& name, const std::optional<std::vector<uint8_t>>& value) {
    if (!value) return SetNull(name);
    Check(VgiBatchSetBytes(&batch_, name.c_str(), value->data(), static_cast<int64_t>(value->size())),
          "set binary field " + name);
}
void Batch::SetBytes(const std::string& name, const std::optional<std::string>& value) {
    if (!value) return SetNull(name);
    Check(VgiBatchSetBytes(&batch_, name.c_str(), value->data(), static_cast<int64_t>(value->size())),
          "set binary field " + name);
}
void Batch::SetInt(const std::string& name, const std::optional<int64_t>& value) {
    if (!value) return SetNull(name);
    Check(VgiBatchSetInt(&batch_, name.c_str(), *value), "set integer field " + name);
}
void Batch::SetDouble(const std::string& name, const std::optional<double>& value) {
    if (!value) return SetNull(name);
    Check(ArrowArrayAppendDouble(MutableColumn(batch_, name), *value), "set double field " + name);
}
void Batch::SetBool(const std::string& name, const std::optional<bool>& value) {
    SetInt(name, value ? std::optional<int64_t>(*value ? 1 : 0) : std::nullopt);
}
void Batch::SetStringList(const std::string& name,
                          const std::optional<std::vector<std::string>>& values) {
    auto* list = MutableColumn(batch_, name);
    if (!values) return Check(ArrowArrayAppendNull(list, 1), "set null string list " + name);
    for (const auto& value : *values)
        Check(VgiArrayAppendString(list->children[0],
                                   {value.data(), static_cast<int64_t>(value.size())}),
              "append string list value " + name);
    Check(ArrowArrayFinishElement(list), "finish string list " + name);
}
void Batch::SetBinaryList(const std::string& name,
                          const std::optional<std::vector<std::vector<uint8_t>>>& values) {
    auto* list = MutableColumn(batch_, name);
    if (!values) return Check(ArrowArrayAppendNull(list, 1), "set null binary list " + name);
    for (const auto& value : *values)
        Check(ArrowArrayAppendBytes(list->children[0], {{value.data()}, static_cast<int64_t>(value.size())}),
              "append binary list value " + name);
    Check(ArrowArrayFinishElement(list), "finish binary list " + name);
}
void Batch::SetInt64List(const std::string& name,
                         const std::optional<std::vector<int64_t>>& values) {
    auto* list = MutableColumn(batch_, name);
    if (!values) return Check(ArrowArrayAppendNull(list, 1), "set null integer list " + name);
    for (auto value : *values)
        Check(ArrowArrayAppendInt(list->children[0], value), "append integer list value " + name);
    Check(ArrowArrayFinishElement(list), "finish integer list " + name);
}
void Batch::SetNullStruct(const std::string& name) {
    Check(ArrowArrayAppendNull(MutableColumn(batch_, name), 1), "set null struct " + name);
}
void Batch::SetStruct(
    const std::string& name,
    const std::vector<std::optional<std::pair<Column, int64_t>>>& values) {
    auto* output = MutableColumn(batch_, name);
    const auto* output_schema = batch_.schema.children[VgiFieldIndex(&batch_.schema, name.c_str())];
    if (!output_schema || output_schema->n_children != static_cast<int64_t>(values.size()))
        throw std::runtime_error("struct value count does not match field " + name);
    for (size_t i = 0; i < values.size(); ++i) {
        if (!values[i]) {
            Check(ArrowArrayAppendNull(output->children[i], 1), "append null struct child " + name);
            continue;
        }
        const auto& [column, row] = *values[i];
        ArrowError error;
        ArrowErrorInit(&error);
        Check(VgiArrayAppendValue(output->children[i], output_schema->children[i], column.view,
                                  column.schema, row, &error),
              error, "append struct child " + name);
    }
    Check(ArrowArrayFinishElement(output), "finish struct field " + name);
}
void Batch::AppendScalar(int64_t column, const Scalar& value) {
    if (column < 0 || column >= batch_.array.n_children)
        throw std::runtime_error("batch scalar column out of range");
    AppendScalarValue(batch_.array.children[column], *batch_.schema.children[column], value);
}
void Batch::AppendColumnValue(int64_t column, const Column& value, int64_t row) {
    if (column < 0 || column >= batch_.array.n_children || !value.valid())
        throw std::runtime_error("invalid batch column append");
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiArrayAppendValue(batch_.array.children[column], batch_.schema.children[column], value.view,
                              value.schema, row, &error),
          error, "append Arrow column value");
}
void Batch::FinishRow() {
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiBatchFinishRow(&batch_, &error), error, "finish batch row");
}
void Batch::Finish() {
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiBatchFinish(&batch_, &error), error, "finish batch");
}

std::string EncodeBatch(const Batch& batch) {
    ArrowBuffer out; ArrowBufferInit(&out);
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiIpcWriteBatch(&batch.raw().schema, &batch.raw().array, nullptr, &out, &error), error,
          "encode IPC batch");
    std::string result(reinterpret_cast<const char*>(out.data), static_cast<size_t>(out.size_bytes));
    ArrowBufferReset(&out);
    return result;
}
BatchPtr DecodeBatch(const std::string& bytes) {
    if (bytes.empty()) return nullptr;
    return Batch::Decode(bytes.data(), static_cast<int64_t>(bytes.size()));
}
std::string EncodeSchema(const Schema& schema) {
    ArrowBuffer out; ArrowBufferInit(&out);
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiIpcWriteSchema(&schema.raw(), &out, &error), error, "encode IPC schema");
    std::string result(reinterpret_cast<const char*>(out.data), static_cast<size_t>(out.size_bytes));
    ArrowBufferReset(&out);
    return result;
}
SchemaPtr DecodeSchema(const std::string& bytes) {
    ArrowSchema schema{};
    ArrowError error; ArrowErrorInit(&error);
    Check(VgiIpcReadSchema({{bytes.data()}, static_cast<int64_t>(bytes.size())}, &schema, &error), error,
          "decode IPC schema");
    return Schema::Adopt(&schema);
}

}  // namespace vgi_sqlite::nano
