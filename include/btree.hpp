#pragma once

#include "../include/btree_node.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/file_cache.hpp"


struct btree_node_info
{
    far_offset_ptr node_offset;
    uint16_t btree_position;
    uint16_t btree_size;

    std::vector<uint8_t> key;
    bool is_found;
    bool is_full;
    bool is_leaf;
    bool is_root;

    bool operator==(const btree_node_info& other) const = default;
};

struct btree_iterator
{
    far_offset_ptr btree_offset; // Offset of the B-tree in the file cache
    std::vector<btree_node_info> path;

    inline bool operator==(const btree_iterator& other) const = default;

    inline bool is_end() const
    {
        for(const auto& p : path)
        {
            if (p.btree_position < p.btree_size)
            {
                return false; // If any node in the path is not at the end, this iterator is not at the end
            }
        }
        return true;
    }
};

class btree_operations
{
public:
    static btree_iterator begin(file_cache& cache, far_offset_ptr btree_offset);
    static btree_iterator end(file_cache& cache, far_offset_ptr btree_offset);

    static btree_iterator seek_begin(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key);
    static btree_iterator seek_end(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key);

    static btree_iterator next(file_cache& cache, far_offset_ptr btree_offset, btree_iterator it);
    static btree_iterator prev(file_cache& cache, far_offset_ptr btree_offset, btree_iterator it);

    static btree_iterator upsert(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key, std::span<uint8_t> value);

    static std::vector<uint8_t> get_entry(file_cache& cache, btree_iterator it);

    static btree_iterator insert(file_cache& cache, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value);
    static btree_iterator update(file_cache& cache, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value);
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

    btree_iterator begin(); // Seek to the first entry in the B-tree (this could be end if the B-tree is empty)
    btree_iterator seek_begin(std::span<uint8_t> key); // seek to the first entry that is greater than or equal to the key
    btree_iterator seek_end(std::span<uint8_t> key); // seek to the first entry that is greater than the key
    btree_iterator end(); // create an iterator that points to the end of the B-tree (not a valid entry)

    btree_iterator next(btree_iterator it); // move to the next entry in the B-tree
    btree_iterator prev(btree_iterator it); // move to the previous entry in the B-tree
    btree_iterator upsert(std::span<uint8_t> key, std::span<uint8_t> value); // insert or update an entry in the B-tree
    std::vector<uint8_t> get_entry(btree_iterator it); // get the entry at the current iterator position
    btree_iterator insert(btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value); // insert an entry at the current iterator position
    btree_iterator update(btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value); // update the entry at the current iterator position
    btree_iterator remove(btree_iterator it); // remove the entry at the current iterator position
};