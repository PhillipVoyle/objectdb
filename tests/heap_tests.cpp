#include <gtest/gtest.h>
#include <vector>
#include <map>
#include <cstring>
#include "../include/file_cache_heap.hpp"

// Remove local constexprs and use file_cache_heap::BLOCK_SIZE, etc.

// Minimal mock file_cache for in-memory testing
class mock_file_cache : public concrete_file_cache {
public:
    std::map<filesize_t, std::vector<uint8_t>> storage;

    mock_file_cache() : concrete_file_cache(std::filesystem::path()) {}

    void write_bytes(filesize_t file_id, filesize_t offset, std::span<const uint8_t> data) override {
        auto& vec = storage[file_id];
        if (vec.size() < offset + data.size()) {
            vec.resize(offset + data.size(), 0);
        }
        std::memcpy(vec.data() + offset, data.data(), data.size());
    }

    void read_bytes(filesize_t file_id, filesize_t offset, std::span<uint8_t> data) override {
        auto it = storage.find(file_id);
        if (it != storage.end() && it->second.size() >= offset + data.size()) {
            std::memcpy(data.data(), it->second.data() + offset, data.size());
        } else {
            std::fill(data.begin(), data.end(), 0);
        }
    }
};

// Minimal mock file_allocator for in-memory testing
class mock_file_allocator : public concrete_file_allocator {
    filesize_t next_file_id = 1;
    filesize_t next_offset = 0;
    mock_file_cache& cache_;
    filesize_t txn_id = 42;
public:
    mock_file_allocator(mock_file_cache& cache)
        : concrete_file_allocator(cache), cache_(cache) {}

    filesize_t get_current_transaction_id() override { return txn_id; }
    far_offset_ptr allocate_block(filesize_t) override {
        far_offset_ptr ptr(next_file_id, next_offset);
        next_offset += 4096;
        return ptr;
    }
    mock_file_cache& get_cache() { return cache_; }
};

TEST(FileCacheHeapTest, AllocateWriteRead) {
    mock_file_cache cache;
    mock_file_allocator allocator(cache);
    file_cache_heap heap(allocator);

    // Allocate an entry
    far_offset_ptr entry = heap.heap_allocate();
    EXPECT_EQ(entry.get_file_id(), 1);
    EXPECT_EQ(entry.get_offset(), 0);

    // Write data to entry
    std::vector<uint8_t> data(file_cache_heap::ENTRY_SIZE, 0xAB);
    heap.write_heap(entry, data);

    // Read back and verify
    std::vector<uint8_t> read = heap.read_heap(entry);
    EXPECT_EQ(read.size(), file_cache_heap::ENTRY_SIZE);
    for (auto b : read) EXPECT_EQ(b, 0xAB);
}

TEST(FileCacheHeapTest, FreeAndReuse) {
    mock_file_cache cache;
    mock_file_allocator allocator(cache);
    file_cache_heap heap(allocator);

    // Allocate two entries
    far_offset_ptr entry1 = heap.heap_allocate();
    far_offset_ptr entry2 = heap.heap_allocate();

    EXPECT_NE(entry1, entry2);

    // Free the first entry
    heap.heap_free(entry1);

    // Next allocation should reuse entry1
    far_offset_ptr entry3 = heap.heap_allocate();
    EXPECT_EQ(entry3, entry1);
}

TEST(FileCacheHeapTest, BlockOverflowAllocatesNewBlock) {
    mock_file_cache cache;
    mock_file_allocator allocator(cache);
    file_cache_heap heap(allocator);

    std::vector<far_offset_ptr> entries;
    // Allocate ENTRIES_PER_BLOCK + 1 entries to force a new block
    for (size_t i = 0; i < file_cache_heap::ENTRIES_PER_BLOCK + 1; ++i) {
        entries.push_back(heap.heap_allocate());
    }

    // The first ENTRIES_PER_BLOCK entries should be in the first block (file_id=1)
    for (size_t i = 0; i < file_cache_heap::ENTRIES_PER_BLOCK; ++i) {
        EXPECT_EQ(entries[i].get_file_id(), 1);
        EXPECT_EQ(entries[i].get_offset(), i * file_cache_heap::ENTRY_SIZE);
    }

    // The 17th entry should be in a new block (file_id=1, offset=4096)
    EXPECT_EQ(entries[file_cache_heap::ENTRIES_PER_BLOCK].get_file_id(), 1);
    EXPECT_EQ(entries[file_cache_heap::ENTRIES_PER_BLOCK].get_offset(), file_cache_heap::BLOCK_SIZE);
}

