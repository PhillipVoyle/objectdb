#pragma once

#include "../include/heap.hpp"
#include "../include/file_allocator.hpp"

class file_cache_heap : public heap
{
    far_offset_ptr heap_root_;
    file_allocator& allocator_;
public:
    file_cache_heap(file_allocator& allocator, far_offset_ptr heap_root = far_offset_ptr());

    virtual far_offset_ptr heap_allocate() = 0;
    virtual void heap_free(far_offset_ptr location) = 0;
    virtual std::vector<uint8_t> read_heap(far_offset_ptr location) = 0;
    virtual void write_heap(far_offset_ptr location, std::span<uint8_t>) = 0;
};