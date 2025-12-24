/* Mock arrow/api.h for syntax verification */
#pragma once
#include <memory>
#include <string>
#include <vector>

namespace arrow {
    class Status {
    public:
        static Status OK() { return Status(true); }
        static Status OutOfMemory(const std::string& msg) { return Status(false); }
        bool ok() const { return ok_; }
        void ValueOrDie() {}
        bool ValueOrDie() const { return true; }
    private:
        Status(bool ok) : ok_(ok) {}
        bool ok_;
    };

    template<typename T>
    class Result {
    public:
        Result(T val) : val_(val) {}
        T ValueOrDie() { return val_; }
        bool ok() const { return true; }
    private:
        T val_;
    };

    class MemoryPool {
    public:
        virtual ~MemoryPool() {}
        virtual Status Allocate(int64_t size, uint8_t** out) = 0;
        virtual Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) = 0;
        virtual void Free(uint8_t* buffer, int64_t size) = 0;
        virtual int64_t bytes_allocated() const = 0;
        virtual int64_t max_memory() const = 0;
        virtual std::string backend_name() const = 0;
    };
    MemoryPool* default_memory_pool();

    class DataType { public: virtual ~DataType() {} };
    std::shared_ptr<DataType> int32();
    std::shared_ptr<DataType> int64();
    std::shared_ptr<DataType> int16();
    std::shared_ptr<DataType> int8();
    std::shared_ptr<DataType> float64();
    std::shared_ptr<DataType> utf8();
    std::shared_ptr<DataType> date32();
    std::shared_ptr<DataType> decimal128(int p, int s);

    class Field {
    public:
        static std::shared_ptr<Field> Make(const std::string& name, std::shared_ptr<DataType> type);
    };
    std::shared_ptr<Field> field(const std::string& name, std::shared_ptr<DataType> type);

    class Schema {
    public:
        Schema(const std::vector<std::shared_ptr<Field>>& fields);
    };

    class Array { public: virtual ~Array() {} };
    class RecordBatch {
    public:
        static std::shared_ptr<RecordBatch> Make(std::shared_ptr<Schema> schema, int64_t num_rows, std::vector<std::shared_ptr<Array>> columns);
    };

    class ArrayBuilder {
    public:
        ArrayBuilder(MemoryPool* pool) {}
        virtual ~ArrayBuilder() {}
        Status Finish(std::shared_ptr<Array>* out) { return Status::OK(); }
        int64_t length() const { return 0; }
        virtual Status AppendNull() = 0;
    };

    class Int32Builder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(int32_t val) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };

    class Int64Builder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(int64_t val) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };
    
    class Int16Builder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(int16_t val) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };
    
    class Int8Builder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(int8_t val) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };

    class DoubleBuilder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(double val) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };

    class StringBuilder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(const char* val, int len) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };

    class Date32Builder : public ArrayBuilder {
    public:
        using ArrayBuilder::ArrayBuilder;
        Status Append(int32_t days) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };

    class Decimal128 {
    public:
        static Result<Decimal128> FromLittleEndian(const uint8_t* bytes, int length) { return Decimal128(); }
        void ValueOrDie() {}
        Decimal128 ValueOrDie() const { return *this; }
    };

    class Decimal128Builder : public ArrayBuilder {
    public:
        Decimal128Builder(std::shared_ptr<DataType> type, MemoryPool* pool) : ArrayBuilder(pool) {}
        Status Append(Decimal128 val) { return Status::OK(); }
        Status AppendNull() override { return Status::OK(); }
    };
}
