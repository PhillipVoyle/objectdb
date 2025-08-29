#pragma once

#include "../include/core.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/file_cache.hpp"

class file_allocator
{
public:
    virtual ~file_allocator() = default;
    virtual file_cache& get_cache() = 0;

    virtual filesize_t get_current_transaction_id() = 0;
    virtual filesize_t create_transaction() = 0;
    virtual far_offset_ptr allocate_block(filesize_t transaction_id) = 0;
};

class concrete_file_allocator : public file_allocator
{
    file_cache& cache_;
public:
    explicit concrete_file_allocator(file_cache& cache);

    file_cache& get_cache() override;

    filesize_t get_current_transaction_id() override;
    filesize_t create_transaction() override;
    far_offset_ptr allocate_block(filesize_t transaction_id) override;
};
