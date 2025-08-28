#pragma once

#include "../include/core.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/file_cache.hpp"

class file_allocator
{
    file_cache& cache_;
public:
    explicit file_allocator(file_cache& cache);

    file_cache& get_cache();

    filesize_t get_current_transaction_id();
    filesize_t create_transaction();
    far_offset_ptr allocate_block(filesize_t transaction_id);
};