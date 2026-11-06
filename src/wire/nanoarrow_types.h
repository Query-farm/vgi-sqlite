// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <nanoarrow/nanoarrow.h>
#include <vgi/batch.h>

namespace vgi_sqlite::nano {

[[noreturn]] void ThrowError(int code, const ArrowError& error, const std::string& operation);
void Check(int code, const std::string& operation);
void Check(int code, const ArrowError& error, const std::string& operation);

class Schema {
public:
    Schema();
    ~Schema();
    Schema(const Schema&) = delete;
    Schema& operator=(const Schema&) = delete;
    Schema(Schema&& other) noexcept;
    Schema& operator=(Schema&& other) noexcept;

    static std::shared_ptr<Schema> Adopt(ArrowSchema* schema);
    static std::shared_ptr<Schema> Copy(const ArrowSchema& schema);
    static std::shared_ptr<Schema> Named(const char* name);

    ArrowSchema& raw() { return schema_; }
    const ArrowSchema& raw() const { return schema_; }
    int64_t num_fields() const { return schema_.n_children; }
    const ArrowSchema* field(int64_t index) const;
    const ArrowSchema* field(const std::string& name) const;
    std::shared_ptr<Schema> Select(const std::vector<int>& indices) const;
    bool Equals(const Schema& other, bool metadata = false) const;
    std::string ToString() const;
    void SetMetadata(const std::vector<std::pair<std::string, std::string>>& metadata);

private:
    ArrowSchema schema_{};
};

using SchemaPtr = std::shared_ptr<Schema>;

SchemaPtr Type(enum ArrowType type);
SchemaPtr ListType(const SchemaPtr& value_type, bool large = false);
SchemaPtr StructType(const std::vector<SchemaPtr>& fields);
SchemaPtr DictionaryType(enum ArrowType index_type, const SchemaPtr& value_type);
SchemaPtr Field(std::string name, const SchemaPtr& type, bool nullable = true,
                const std::vector<std::pair<std::string, std::string>>& metadata = {});
SchemaPtr MakeSchema(const std::vector<SchemaPtr>& fields);
enum ArrowType TypeId(const ArrowSchema& schema);
std::string TypeName(const ArrowSchema& schema);
std::optional<std::string> MetadataValue(const ArrowSchema& schema, const std::string& key);

struct Scalar {
    enum class Kind { Null, Signed, Unsigned, Double, Bool, String, Binary };
    SchemaPtr type;
    Kind kind = Kind::Null;
    std::variant<std::monostate, int64_t, uint64_t, double, bool, std::string,
                 std::vector<uint8_t>> value;

    static std::shared_ptr<Scalar> Null(const SchemaPtr& type);
    static std::shared_ptr<Scalar> Signed(int64_t value, const SchemaPtr& type = {});
    static std::shared_ptr<Scalar> Unsigned(uint64_t value, const SchemaPtr& type = {});
    static std::shared_ptr<Scalar> Real(double value, const SchemaPtr& type = {});
    static std::shared_ptr<Scalar> Boolean(bool value, const SchemaPtr& type = {});
    static std::shared_ptr<Scalar> String(std::string value, const SchemaPtr& type = {});
    static std::shared_ptr<Scalar> Binary(std::vector<uint8_t> value, const SchemaPtr& type = {});
};

using ScalarPtr = std::shared_ptr<Scalar>;

struct Column {
    const ArrowArrayView* view = nullptr;
    const ArrowSchema* schema = nullptr;

    bool valid() const { return view && schema; }
    int64_t length() const { return view ? view->length : 0; }
    bool IsNull(int64_t row) const;
    enum ArrowType type_id() const;
    std::string TypeString() const;
    Column child(int64_t index) const;
    Column dictionary() const;
    int64_t Int(int64_t row) const;
    uint64_t UInt(int64_t row) const;
    double Double(int64_t row) const;
    bool Bool(int64_t row) const;
    std::string String(int64_t row) const;
    std::vector<uint8_t> Bytes(int64_t row) const;
    int64_t Offset(int64_t row) const;
};

class Batch {
public:
    Batch();
    ~Batch();
    Batch(const Batch&) = delete;
    Batch& operator=(const Batch&) = delete;
    Batch(Batch&& other) noexcept;
    Batch& operator=(Batch&& other) noexcept;

    static std::shared_ptr<Batch> Create(const SchemaPtr& schema);
    static std::shared_ptr<Batch> Named(const char* name);
    static std::shared_ptr<Batch> Decode(const void* data, int64_t size);
    static std::shared_ptr<Batch> Adopt(VgiBatch* batch);
    static std::shared_ptr<Batch> FromScalars(const SchemaPtr& schema,
                                              const std::vector<ScalarPtr>& values);

    VgiBatch& raw() { return batch_; }
    const VgiBatch& raw() const { return batch_; }
    int64_t num_rows() const { return batch_.array.length; }
    int64_t num_columns() const { return batch_.schema.n_children; }
    SchemaPtr schema() const { return Schema::Copy(batch_.schema); }
    Column column(int64_t index) const;
    Column column(const std::string& name) const;

    void SetNull(const std::string& name);
    void SetString(const std::string& name, const std::optional<std::string>& value);
    void SetBytes(const std::string& name, const std::optional<std::vector<uint8_t>>& value);
    void SetBytes(const std::string& name, const std::optional<std::string>& value);
    void SetInt(const std::string& name, const std::optional<int64_t>& value);
    void SetDouble(const std::string& name, const std::optional<double>& value);
    void SetBool(const std::string& name, const std::optional<bool>& value);
    void SetStringList(const std::string& name,
                       const std::optional<std::vector<std::string>>& values);
    void SetBinaryList(const std::string& name,
                       const std::optional<std::vector<std::vector<uint8_t>>>& values);
    void SetInt64List(const std::string& name,
                      const std::optional<std::vector<int64_t>>& values);
    void SetNullStruct(const std::string& name);
    void SetStruct(const std::string& name,
                   const std::vector<std::optional<std::pair<Column, int64_t>>>& values);
    void AppendScalar(int64_t column, const Scalar& value);
    void AppendColumnValue(int64_t column, const Column& value, int64_t row);
    void FinishRow();
    void Finish();

private:
    VgiBatch batch_{};
};

using BatchPtr = std::shared_ptr<Batch>;

std::string EncodeBatch(const Batch& batch);
BatchPtr DecodeBatch(const std::string& bytes);
std::string EncodeSchema(const Schema& schema);
SchemaPtr DecodeSchema(const std::string& bytes);

}  // namespace vgi_sqlite::nano
