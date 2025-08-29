#pragma once

#include "../include/heap.hpp"
#include "../include/file_allocator.hpp"

class file_cache_heap : public heap
{
    far_offset_ptr heap_root_;
    file_allocator& allocator_;
public:
    static constexpr size_t BLOCK_SIZE = 4096;
    static constexpr size_t ENTRY_SIZE = 256;
    static constexpr size_t ENTRIES_PER_BLOCK = BLOCK_SIZE / ENTRY_SIZE;

    file_cache_heap(file_allocator& allocator, far_offset_ptr heap_root = far_offset_ptr());

    far_offset_ptr heap_allocate() override;
    void heap_free(far_offset_ptr location) override;
    std::vector<uint8_t> read_heap(far_offset_ptr location) override;
    void write_heap(far_offset_ptr location, std::span<uint8_t>) override;
};