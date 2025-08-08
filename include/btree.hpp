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

    btree_iterator internal_insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value);
    btree_iterator internal_update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value);
    btree_iterator internal_remove(filesize_t transaction_id, btree_iterator it);

public:

    far_offset_ptr get_offset() const { return offset_; }
    btree(std::shared_ptr<btree_row_traits> row_traits, file_cache& cache, far_offset_ptr offset, file_allocator& allocator);

    btree_iterator begin(); // Seek to the first entry in the B-tree (this could be end if the B-tree is empty)

    template<typename TSpanComparitor>
    btree_iterator seek_begin(std::span<uint8_t> key, TSpanComparitor comparitor) // seek to the first entry that is greater than or equal to the key
    {
        if (offset_.get_file_id() == 0 && offset_.get_offset() == 0)
        {
            return btree_iterator{}; // Invalid B-tree offset
        }

        btree_iterator result;
        btree_node node(*this);
        auto current_offset = offset_;
        for (;;)
        {
            auto iterator = cache_.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
            if (!iterator.has_next())
            {
                if (result.path.empty())
                {
                    result.btree_offset = offset_;
                    result.path.clear();
                    return result; // Empty B-tree
                }
                else
                {
                    throw object_db_exception("B-tree node is empty or corrupted.");
                }
            }
            node.read(iterator);
            auto find_result = node.find_key(key, comparitor);
            btree_node_info info;
            info.node_offset = current_offset;
            info.btree_size = node.get_entry_count();

            uint16_t read_key_position = (find_result.found || find_result.position == 0)
                ? find_result.position
                : (find_result.position - 1);

            if (node.is_leaf())
            {
                info.btree_position = (uint16_t)find_result.position;
                info.is_found = find_result.found;
            }
            else
            {
                info.btree_position = read_key_position;
                info.is_found = true; // In a branch node, we always find the key or the position where it would be inserted
            }

            result.path.push_back(info);

            if (node.is_leaf())
            {
                break;
            }
            else
            {
                auto offset_span = node.get_value_at(read_key_position);
                auto span_it = span_iterator(offset_span);
                current_offset.read(span_it);
            }
        }
        result.btree_offset = offset_;
        return result;

    }

    template<typename TSpanComparitor>
    btree_iterator seek_end(std::span<uint8_t> key, TSpanComparitor comparitor) // seek to the first entry that is greater than the key
    {
        if (offset_.get_file_id() == 0 && offset_.get_offset() == 0)
        {

            btree_iterator result{}; // Invalid B-tree offset
            result.btree_offset = offset_;
            return result;
        }

        auto it = seek_begin(cache_, offset_, key, comparitor);
        if (it.is_end())
        {
            return it; // If the key is not found, return end iterator
        }
        // Move to the next entry if the key is found
        if (it.path.back().is_found)
        {
            // Move to the next entry in the B-tree
            it = next(cache_, offset_, it);
        }
        else {
            // If the key was not found, we return the iterator at the position where it would be inserted
            // This is already handled by seek_begin, so we just return it
        }

        it.btree_offset = offset_;
        return it;
    }

    btree_iterator end(); // create an iterator that points to the end of the B-tree (not a valid entry)

    btree_iterator next(btree_iterator it); // move to the next entry in the B-tree
    btree_iterator prev(btree_iterator it); // move to the previous entry in the B-tree

    template<typename TSpanComparitor>
    btree_iterator upsert(filesize_t transaction_id, std::span<uint8_t> key, std::span<uint8_t> value, TSpanComparitor comparitor) // insert or update an entry in the B-tree
    {
        btree_iterator it = seek_begin(key, comparitor);
        if (it.is_end() || !it.path.back().is_found)
        {
            // If the key is not found, we need to insert it
            return insert(transaction_id, it, key, value);
        }
        else
        {
            // If the key is found, we can update it
            return update(transaction_id, it, key, value);
        }
    }

    std::vector<uint8_t> get_entry(btree_iterator it); // get the entry at the current iterator position
    btree_iterator insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value); // insert an entry at the current iterator position
    btree_iterator update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value); // update the entry at the current iterator position
    btree_iterator remove(filesize_t transaction_id, btree_iterator it); // remove the entry at the current iterator position
};