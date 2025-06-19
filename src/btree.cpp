
#include "../include/btree.hpp"
#include <cassert>

btree::btree(file_cache& cache, far_offset_ptr offset) : cache_(cache), offset_(offset)
{
}
btree_iterator btree::begin()
{
    return btree_operations::begin(cache_, offset_);
}

btree_iterator btree::end()
{
    return btree_operations::begin(cache_, offset_);
}

btree_iterator btree::seek_begin(std::span<uint8_t> key)
{
    return btree_operations::seek_begin(cache_, offset_, key);
}
btree_iterator btree::seek_end(std::span<uint8_t> key)
{
    return btree_operations::seek_end(cache_, offset_, key);
}

btree_iterator btree::next(btree_iterator it)
{
    return btree_operations::next(cache_, offset_, it);
}
btree_iterator btree::prev(btree_iterator it)
{
    assert(it.btree_offset == offset_); //"Iterator must be from the same B-tree instance.";
    return btree_operations::prev(cache_, offset_, it);
}

btree_iterator btree::upsert(std::span<uint8_t> entry)
{
    return btree_operations::upsert(cache_, offset_, entry);
}

std::vector<uint8_t> btree::get_entry(btree_iterator it)
{
    return btree_operations::get_entry(cache_, it);
}

btree_iterator btree::insert(btree_iterator it, std::span<uint8_t> entry)
{
    return btree_operations::insert(cache_, it, entry);
}
btree_iterator btree::update(btree_iterator it, std::span<uint8_t> entry)
{
    return btree_operations::update(cache_, it, entry);
}
btree_iterator btree::remove(btree_iterator it)
{
    return btree_operations::remove(cache_, it);
}

btree_iterator btree_operations::begin(file_cache& cache, far_offset_ptr btree_offset)
{
    btree_iterator result;
    btree_node node;
    auto current_offset = btree_offset;
    for (;;)
    {
        auto iterator = cache.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
        node.read(iterator);
        auto md = node.get_metadata();

        btree_node_info info;
        info.node_offset = current_offset;
        info.btree_position = 0;
        info.found = 0;
        if (node.get_entry_count() > 0)
        {
            info.found = 1;
        }
        else
        {
            info.found = 0;
            return result; // empty btree
        }

        auto span = node.get_key_at(0);
        info.key.resize(span.size());
        std::copy(span.begin(), span.end(), info.key.begin());

        result.path.push_back(info);

        if (node.is_leaf())
        {
            break;
        }
        else
        {
            node.get_value_at(0);
        }
    }
    return result;
}

btree_iterator btree_operations::end(far_offset_ptr btree_offset)
{
    btree_iterator result;
    result.btree_offset = btree_offset;
    return result;
}

btree_iterator btree_operations::seek_begin(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key)
{
    btree_iterator result;
    btree_node node;
    auto current_offset = btree_offset;
    for (;;)
    {
        auto iterator = cache.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
        node.read(iterator);
        auto md = node.get_metadata();
        auto find_result = node.find_key(md, key);
        btree_node_info info;
        info.node_offset = current_offset;
        info.btree_position = find_result.position;
        info.found = find_result.found;
        auto span = node.get_key_at(find_result.position);
        info.key.resize(span.size());
        std::copy(span.begin(), span.end(), info.key.begin());

        result.path.push_back(info);

        if (node.is_leaf())
        {
            break;
        }
        else
        {
            node.get_value_at(find_result.position);
        }
    }
    return result;
}

btree_iterator btree_operations::seek_end(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key)
{
    auto it = seek_begin(cache, btree_offset, key);
    if (it.is_end())
    {
        return it; // If the key is not found, return end iterator
    }
    // Move to the next entry if the key is found
    if (it.path.back().found)
    {
        // Move to the next entry in the B-tree
        it = next(cache, btree_offset, it);
    } else {
        // If the key was not found, we return the iterator at the position where it would be inserted
        // This is already handled by seek_begin, so we just return it
    }
    return it;
}

btree_iterator btree_operations::next(file_cache& cache, far_offset_ptr btree_offset, btree_iterator it)
{
    btree_iterator result = it;
    // If iterator is at end, return end iterator
    if (result.is_end()) 
    {
        return end(btree_offset);
    }
    // Traverse up the path to find a node with a next entry
    while (!result.path.empty())
    {
        btree_node_info& info = result.path.back();
        btree_node node;
        auto iterator = cache.get_iterator(info.node_offset.get_file_id(), info.node_offset.get_offset());
        node.read(iterator);
        if (info.btree_position + 1 < node.get_entry_count())
        {
            // Move to next entry in this node
            ++info.btree_position;
            // Update key in leaf node
            auto key_span = node.get_key_at(info.btree_position);
            info.key.assign(key_span.begin(), key_span.end());
            info.found = true;
            return result;
        }
        else
        {
            // No next entry in this node, move up
            result.path.pop_back();
        }
    }
    // If we reach here, there is no next entry (we were at the last entry)
    return end(btree_offset);
}
btree_iterator btree_operations::prev(file_cache& cache, far_offset_ptr btree_offset, btree_iterator it)
{
    btree_iterator result = it;

    // If iterator is at end, seek to the last record if possible
    if (result.is_end()) {
        // Seek to the rightmost (last) record in the B-tree
        result.path.clear();
        btree_node node;
        // The root offset is not stored in the iterator, so we need to get it from the first node in the path if available
        // If the iterator is at end and has no path, we cannot proceed
        if (it.path.empty()) {
            return btree_iterator{};
        }
        far_offset_ptr current_offset = it.path.front().node_offset;
        for (;;) {
            auto iterator = cache.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
            node.read(iterator);
            btree_node_info info;
            info.node_offset = current_offset;
            info.btree_position = node.get_entry_count() ? node.get_entry_count() - 1 : 0;
            info.found = node.get_entry_count() > 0;
            if (!info.found)
            {
                return end(btree_offset);
            }
            auto key_span = node.get_key_at(info.btree_position);
            info.key.assign(key_span.begin(), key_span.end());
            result.path.push_back(info);
            if (node.is_leaf())
            {
                return result;
            }
            // Descend to the rightmost child
            auto child_offset_span = node.get_value_at(node.get_entry_count());
            far_offset_ptr child_offset;
            std::copy(child_offset_span.begin(), child_offset_span.end(), reinterpret_cast<uint8_t*>(&child_offset));
            current_offset = child_offset;
        }
    }

    // Traverse up the path to find a node with a previous entry
    while (!result.path.empty()) {
        btree_node_info& info = result.path.back();
        if (info.btree_position > 0) {
            // Move to previous entry in this node
            --info.btree_position;

            // Descend to the rightmost leaf if not a leaf node
            btree_node node;
            auto iterator = cache.get_iterator(info.node_offset.get_file_id(), info.node_offset.get_offset());
            node.read(iterator);

            while (!node.is_leaf()) {
                // Get the child pointer at btree_position + 1 (for prev, we want the right child of the previous key)
                auto child_offset_span = node.get_value_at(info.btree_position + 1);
                far_offset_ptr child_offset;
                std::copy(child_offset_span.begin(), child_offset_span.end(), reinterpret_cast<uint8_t*>(&child_offset));
                btree_node_info child_info;
                child_info.node_offset = child_offset;
                child_info.btree_position = node.get_entry_count() - 1;
                child_info.found = true;
                auto key_span = node.get_key_at(node.get_entry_count() - 1);
                child_info.key.assign(key_span.begin(), key_span.end());
                result.path.push_back(child_info);

                // Read the child node
                iterator = cache.get_iterator(child_offset.get_file_id(), child_offset.get_offset());
                node.read(iterator);
            }

            // Update key in leaf node
            btree_node_info& leaf_info = result.path.back();
            auto key_span = node.get_key_at(leaf_info.btree_position);
            leaf_info.key.assign(key_span.begin(), key_span.end());
            leaf_info.found = true;
            return result;
        } else {
            // No previous entry in this node, move up
            result.path.pop_back();
        }
    }

    // If we reach here, there is no previous entry (we were at the first entry)
    return btree_iterator{};
}

btree_iterator btree_operations::upsert(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> entry)
{
    btree_iterator it = seek_begin(cache, btree_offset, entry);
    if (it.is_end() || !it.path.back().found)
    {
        // If the key is not found, we need to insert it
        return insert(cache, it, entry);
    }
    else
    {
        // If the key is found, we can update it
        return update(cache, it, entry);
    }
}

std::vector<uint8_t> btree_operations::get_entry(file_cache& cache, btree_iterator it)
{
    if (it.is_end())
    {
        throw object_db_exception("Iterator is at end, cannot get entry.");
    }
    if (it.path.empty())
    {
        throw object_db_exception("Iterator path is empty, cannot get entry.");
    }
    if (it.path.back().found)
    {
        btree_node node;
        auto iterator = cache.get_iterator(it.path.back().node_offset.get_file_id(), it.path.back().node_offset.get_offset());
        node.read(iterator);
        auto key = node.get_key_at(it.path.back().btree_position);
        auto value = node.get_value_at(it.path.back().btree_position);
        std::vector<uint8_t> result;
        result.reserve(key.size() + value.size());
        result.insert(result.end(), key.begin(), key.end());
        result.insert(result.end(), value.begin(), value.end());
        return result;
    } else
    {
        throw object_db_exception("Entry not found in the B-tree.");
    }
}

// These three methods need to know whether we are operating within the current transaction or an older transaction
// within a transaction we should modify the btree, outside we should copy the nodes on write
// we can use the following methods to get and set the transaction ID for the btree node
//    uint64_t btree_node::get_transaction_id();
//    void btree_node::set_transaction_id(uint64_t transaction_id);

btree_iterator btree_operations::insert(file_cache& cache, btree_iterator it, std::span<uint8_t> entry)
{
    if (it.is_end())
    {
        auto btree_offset = it.btree_offset;
        // If the iterator is at end, we need to insert at the end of the B-tree
        btree_node node;
        auto iterator = cache.get_iterator(btree_offset.get_file_id(), btree_offset.get_offset());
        node.read(iterator);
        // Check if the node is full, if so we need to split it
        if (node.is_full())
        {
            // Split the node and create a new root if necessary
            // This is a complex operation that involves splitting the node and updating the B-tree structure
            throw object_db_exception("B-tree node split not implemented yet.");
        }
    }
    throw object_db_exception("not implemented");
}
btree_iterator btree_operations::update(file_cache& cache, btree_iterator it, std::span<uint8_t> entry)
{
    throw object_db_exception("not implemented");
}
btree_iterator btree_operations::remove(file_cache& cache, btree_iterator it)
{
    throw object_db_exception("not implemented");
}
