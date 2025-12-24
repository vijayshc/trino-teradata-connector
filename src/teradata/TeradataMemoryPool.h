#pragma once

#include <arrow/memory_pool.h>
#include <sqltypes_td.h>

namespace td_export {

class TeradataMemoryPool : public arrow::MemoryPool {
public:
    TeradataMemoryPool() : bytes_allocated_(0) {}
    ~TeradataMemoryPool() override {}

    arrow::Status Allocate(int64_t size, uint8_t** out) override {
        *out = static_cast<uint8_t*>(FNC_malloc(size));
        if (*out == nullptr) {
            return arrow::Status::OutOfMemory("FNC_malloc failed");
        }
        bytes_allocated_ += size;
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
        // Teradata FNC_malloc doesn't have a direct realloc.
        // We'll mimic it with malloc + memcpy + free.
        uint8_t* new_ptr = static_cast<uint8_t*>(FNC_malloc(new_size));
        if (new_ptr == nullptr) {
            return arrow::Status::OutOfMemory("FNC_malloc failed during reallocate");
        }
        if (*ptr != nullptr) {
            memcpy(new_ptr, *ptr, std::min(old_size, new_size));
            FNC_free(*ptr);
        }
        *ptr = new_ptr;
        bytes_allocated_ += (new_size - old_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size) override {
        if (buffer != nullptr) {
            FNC_free(buffer);
            bytes_allocated_ -= size;
        }
    }

    int64_t bytes_allocated() const override {
        return bytes_allocated_.load();
    }

    int64_t max_memory() const override {
        // Teradata memory tracking is handled by the DB.
        return bytes_allocated_.load();
    }

    std::string backend_name() const override {
        return "Teradata";
    }

private:
    std::atomic<int64_t> bytes_allocated_;
};

} // namespace td_export
