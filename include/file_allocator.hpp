#pragma once

#include "../include/core.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/file_cache.hpp"

class file_allocator
{
    file_cache& cache_;
public:
    explicit file_allocator(file_cache& cache);
    filesize_t create_transaction();
    far_offset_ptr allocate_block(filesize_t transaction_id);
};