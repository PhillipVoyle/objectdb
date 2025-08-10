#pragma once

#include "../include/btree_node.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/file_cache.hpp"
#include "../include/file_allocator.hpp"
#include "../include/btree_row_traits.hpp"

struct btree_node_info
{
    far_offset_ptr node_offset;
    uint16_t btree_position;
    uint16_t btree_size;

    bool is_found;

    bool operator==(const btree_node_info& other) const = default;

    btree_node::find_result get_find_result()
    {
        return btree_node::find_result
        {
            .position = btree_position,
            .found = is_found
        };
    }
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
            if (p.is_found)
            {
                return false; // If the last node is found, this iterator is not at the end
            }
        }
        return true;
    }
};


class btree
{
    btree() = delete;
    btree(const btree& b) = delete;
    void operator=(const btree& b) = delete;
    file_cache& cache_;
    file_allocator& allocator_;
    far_offset_ptr offset_;

    bool check_offset();

    std::shared_ptr<btree_row_traits> row_traits_;

    btree_iterator internal_next(btree_iterator it);
    btree_iterator internal_prev(btree_iterator it);

    std::vector<uint8_t> internal_get_entry(btree_iterator it);

    std::vector<uint8_t> derive_key_from_entry(std::span<uint8_t> entry);

    btree_iterator internal_insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry);
    btree_iterator internal_update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry);
    btree_iterator internal_remove(filesize_t transaction_id, btree_iterator it);

    uint32_t get_key_size();
    uint32_t get_value_size();
    uint32_t get_entry_size();

public:

    std::shared_ptr<btree_row_traits> get_row_traits();

    int compare_keys(std::span<uint8_t> a, std::span<uint8_t> b);

    far_offset_ptr get_offset() const { return offset_; }
    btree(std::shared_ptr<btree_row_traits> row_traits, file_cache& cache, far_offset_ptr offset, file_allocator& allocator);

    btree_iterator begin(); // Seek to the first entry in the B-tree (this could be end if the B-tree is empty)

    btree_iterator seek_begin(std::span<uint8_t> key); // seek to the first entry that is greater than or equal to the key
    btree_iterator seek_end(std::span<uint8_t> key); // seek to the first entry that is greater than the key

    btree_iterator end(); // create an iterator that points to the end of the B-tree (not a valid entry)

    btree_iterator next(btree_iterator it); // move to the next entry in the B-tree
    btree_iterator prev(btree_iterator it); // move to the previous entry in the B-tree

    btree_iterator upsert(filesize_t transaction_id, std::span<uint8_t> entry);

    std::vector<uint8_t> get_entry(btree_iterator it); // get the entry at the current iterator position
    btree_iterator insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry); // insert an entry at the current iterator position
    btree_iterator update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry); // update the entry at the current iterator position
    btree_iterator remove(filesize_t transaction_id, btree_iterator it); // remove the entry at the current iterator position
};