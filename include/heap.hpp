#pragma once

#include "../include/core.hpp"
#include "../include/far_offset_ptr.hpp"

class heap
{
public:
    virtual ~heap() = default;
    virtual far_offset_ptr heap_allocate() = 0;
    virtual void heap_free(far_offset_ptr location) = 0;
    virtual std::vector<uint8_t> read_heap(far_offset_ptr location) = 0;
    virtual void write_heap(far_offset_ptr location, std::span<uint8_t>) = 0;
};