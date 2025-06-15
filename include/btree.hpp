#pragma once

#include "../include/btree_node.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/file_cache.hpp"


struct btree_node_info
{
    far_offset_ptr node_offset;
    uint16_t btree_position;
    std::vector<uint8_t> key;
    bool found;
};

struct btree_iterator
{
    std::vector<btree_node_info> path;
};

class btree_operations
{
public:
    static btree_iterator seek_begin(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key);
    static btree_iterator seek_end(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key);

    static btree_iterator next(file_cache& cache, btree_iterator it);
    static btree_iterator prev(file_cache& cache, btree_iterator it);

    static btree_iterator upsert(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> entry);

    static std::vector<uint8_t> get_entry(file_cache& cache, btree_iterator it);

    static btree_iterator insert(file_cache& cache, btree_iterator it, std::span<uint8_t> entry);
    static btree_iterator update(file_cache& cache, btree_iterator it, std::span<uint8_t> entry);
    static btree_iterator remove(file_cache& cache, btree_iterator it);
};

class btree
{
    btree() = delete;
    btree(const btree& b) = delete;
    void operator=(const btree& b) = delete;
    file_cache& cache_;
    far_offset_ptr offset_;
public:
    btree(file_cache& cache, far_offset_ptr offset);
    btree_iterator seek_begin(std::span<uint8_t> key);
    btree_iterator seek_end(std::span<uint8_t> key);
    btree_iterator next(btree_iterator it);
    btree_iterator prev(btree_iterator it);
    btree_iterator upsert(std::span<uint8_t> entry);
    std::vector<uint8_t> get_entry(btree_iterator it);
    btree_iterator insert(btree_iterator it, std::span<uint8_t> entry);
    btree_iterator update(btree_iterator it, std::span<uint8_t> entry);
    btree_iterator remove(btree_iterator it);
};