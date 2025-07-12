
#include "../include/btree.hpp"
#include "../include/span_iterator.hpp"
#include <cassert>

btree::btree(file_cache& cache, far_offset_ptr offset, file_allocator& allocator, uint32_t key_size, uint32_t value_size):
    cache_(cache),
    allocator_(allocator),
    offset_(offset),
    key_size_(key_size),
    value_size_(value_size)
{
}

bool btree::check_offset()
{
    if (offset_.get_file_id() == 0 && offset_.get_offset() == 0)
    {
        return false; // Invalid offset
    }
    return true;
}

btree_iterator btree::begin()
{
    return btree_operations::begin(cache_, offset_);
}

btree_iterator btree::end()
{
    return btree_operations::end(cache_, offset_);
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

btree_iterator btree::upsert(filesize_t transaction_id, std::span<uint8_t> key, std::span<uint8_t> value)
{
    btree_iterator it = seek_begin(key);
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

std::vector<uint8_t> btree::get_entry(btree_iterator it)
{
    return btree_operations::get_entry(cache_, it);
}

btree_iterator btree::insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value)
{
    auto result =  btree_operations::insert(transaction_id, allocator_, cache_, it, key, value);
    if (result.path.empty())
    {
        throw object_db_exception("B-tree is empty or corrupted, cannot insert entry.");
    }
    offset_ = result.path.front().node_offset; // Update the offset to the new root if it was created
    result.btree_offset = offset_; // Ensure the btree_offset is updated to the current B-tree root
    return result;

}
btree_iterator btree::update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value)
{
    auto result = btree_operations::update(transaction_id, allocator_, cache_, it, key, value);
    if (result.path.empty())
    {
        throw object_db_exception("B-tree is empty or corrupted, cannot update entry.");
    }
    offset_ = result.path.front().node_offset; // Update the offset to the new root if it was created
    result.btree_offset = offset_; // Ensure the btree_offset is updated to the current B-tree root
    return result;
}
btree_iterator btree::remove(filesize_t transaction_id, btree_iterator it)
{
    auto result = btree_operations::remove(transaction_id, allocator_, cache_, it);

    if (result.path.empty())
    {
        offset_ = far_offset_ptr();
    }
    else
    {
        offset_ = result.path.front().node_offset; // Update the offset to the new root if it was created
    }

    result.btree_offset = offset_; // Ensure the btree_offset is updated to the current B-tree root
    return result;
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
        info.is_found = 0;
        info.btree_size = node.get_entry_count();
        if (node.get_entry_count() > 0)
        {
            info.is_found = 1;
            info.is_full = node.is_full();
        }
        else
        {
            info.is_found = 0;
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

btree_iterator btree_operations::end(file_cache& cache, far_offset_ptr btree_offset)
{
    btree_iterator result;
    result.btree_offset = btree_offset;

    far_offset_ptr current_offset = btree_offset;

    for (;;)
    {
        btree_node node;
        auto iterator = cache.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
        node.read(iterator);
        btree_node_info info;
        info.node_offset = current_offset;
        info.btree_position = node.get_entry_count(); // Position after the last entry
        info.btree_size = node.get_entry_count();
        info.is_found = false; // End iterator does not point to a valid entry
        info.is_full = node.is_full();
        result.path.push_back(info);
        if (node.is_leaf())
        {
            break;
        }
        else
        {
            // Move to the rightmost child
            auto child_offset_span = node.get_value_at(node.get_entry_count());
            auto it = span_iterator(child_offset_span);
            current_offset.read(it);
        }
    }

    return result;
}

btree_iterator btree_operations::seek_begin(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key)
{
    if (btree_offset.get_file_id() == 0 && btree_offset.get_offset() == 0)
    {
        return btree_iterator{}; // Invalid B-tree offset
    }

    btree_iterator result;
    btree_node node;
    auto current_offset = btree_offset;
    for (;;)
    {
        auto iterator = cache.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
        if (!iterator.has_next())
        {
            if (result.path.empty())
            {
                result.btree_offset = btree_offset;
                result.path.clear();
                return result; // Empty B-tree
            }
            else
            {
                throw object_db_exception("B-tree node is empty or corrupted.");
            }
        }
        node.read(iterator);
        auto md = node.get_metadata();
        auto find_result = node.find_key(md, key);
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

        // if there's a key, read it
        if (read_key_position < node.get_entry_count())
        {
            auto span = node.get_key_at(read_key_position);
            info.key.resize(span.size());
            std::copy(span.begin(), span.end(), info.key.begin());
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
    result.btree_offset = btree_offset;
    return result;
}

btree_iterator btree_operations::seek_end(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key)
{
    if (btree_offset.get_file_id() == 0 && btree_offset.get_offset() == 0)
    {

        btree_iterator result{}; // Invalid B-tree offset
        result.btree_offset = btree_offset;
        return result;
    }

    auto it = seek_begin(cache, btree_offset, key);
    if (it.is_end())
    {
        return it; // If the key is not found, return end iterator
    }
    // Move to the next entry if the key is found
    if (it.path.back().is_found)
    {
        // Move to the next entry in the B-tree
        it = next(cache, btree_offset, it);
    } else {
        // If the key was not found, we return the iterator at the position where it would be inserted
        // This is already handled by seek_begin, so we just return it
    }

    it.btree_offset = btree_offset;
    return it;
}

btree_iterator btree_operations::next(file_cache& cache, far_offset_ptr btree_offset, btree_iterator it)
{
    if (btree_offset.get_file_id() == 0 && btree_offset.get_offset() == 0)
    {
        btree_iterator result{}; // Invalid B-tree offset
        result.btree_offset = btree_offset;
        return result;
    }
    btree_iterator result = it;
    result.btree_offset = btree_offset;

    // If iterator is at end, return end iterator
    if (result.is_end()) 
    {
        return end(cache, btree_offset);
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
            info.is_found = true;
            info.is_full = node.is_full();

            return result;
        }
        else
        {
            // No next entry in this node, move up
            result.path.pop_back();
        }
    }
    // If we reach here, there is no next entry (we were at the last entry)
    return end(cache, btree_offset);
}
btree_iterator btree_operations::prev(file_cache& cache, far_offset_ptr btree_offset, btree_iterator it)
{
    if (btree_offset.get_file_id() == 0 && btree_offset.get_offset() == 0)
    {
        btree_iterator result{}; // Invalid B-tree offset
        result.btree_offset = btree_offset;
        return result;
    }
    btree_iterator result = it;
    result.btree_offset = btree_offset;

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
            info.is_found = node.get_entry_count() > 0;
            info.is_full = node.is_full();
            info.btree_size = node.get_entry_count();

            if (!info.is_found)
            {
                return end(cache, btree_offset);
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
                child_info.is_found = true;
                child_info.is_full = node.is_full();
                child_info.btree_size = node.get_entry_count();

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
            leaf_info.is_found = true;
            leaf_info.is_full = node.is_full();
            return result;
        } else {
            // No previous entry in this node, move up
            result.path.pop_back();
        }
    }

    // If we reach here, there is no previous entry (we were at the first entry)
    return btree_iterator{};
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
    if (it.path.back().is_found)
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

btree_iterator btree_operations::insert(filesize_t transaction_id, file_allocator& allocator, file_cache& cache, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value)
{
    btree_iterator original = it;
    btree_iterator result = it;
    far_offset_ptr new_or_current_node_offset;
    far_offset_ptr insert_offset;
    bool insert_needed = false;

    std::vector<uint8_t> insert_key;
    std::vector<uint8_t> update_key;

    if (original.path.empty())
    {
        // this is an empty btree
        btree_node node;
        node.init_leaf();
        node.set_transaction_id(transaction_id);
        node.set_key_size(key.size());
        node.set_value_size(value.size());
        auto medatada = node.get_metadata();
        auto find_result = btree_node::find_result{
            .position = 0, // We will insert at the beginning
            .found = false // We are inserting a new key
        };

        std::vector<uint8_t> new_entry(key.size() + value.size());
        std::copy(key.begin(), key.end(), new_entry.begin());
        std::copy(value.begin(), value.end(), new_entry.begin() + key.size());

        node.insert_entry(medatada, find_result, new_entry);
        node.set_transaction_id(transaction_id);
        new_or_current_node_offset = allocator.allocate_block(transaction_id);
        auto write_it = cache.get_iterator(new_or_current_node_offset.get_file_id(), new_or_current_node_offset.get_offset());
        node.write(write_it);
        btree_node_info info;
        info.node_offset = new_or_current_node_offset;
        info.btree_position = 0; // We inserted at the beginning
        info.is_found = true; // We found the entry we just inserted
        info.is_full = node.is_full();
        info.btree_size = node.get_entry_count();
        info.key.assign(key.begin(), key.end()); // Store the key we just inserted
        original.path.push_back(info);
        result = original; // The result is the same as the current iterator
        return result;
    }

    bool expect_leaf = true;
    result = original;
    auto current_path = original.path;
    uint16_t result_btree_position = 0;
    while (!current_path.empty())
    {
        auto node_info = current_path.back();

        current_path.pop_back();
        int path_position = current_path.size();
        btree_node node;
        auto iterator = cache.get_iterator(
            node_info.node_offset.get_file_id(),
            node_info.node_offset.get_offset());

        node.read(iterator);
        auto md = node.get_metadata();
        auto find_result = node_info.get_find_result();
        if (node.is_leaf())
        {
            if (node_info.is_found)
            {
                throw object_db_exception("inserts require that a key does not already exist");
            }

            if (!expect_leaf)
            {
                throw object_db_exception("unexpected leaf node");
            }
            // Insert the entry in the current node
            std::vector<uint8_t> new_entry(node_info.key.size() + value.size());
            std::copy(key.begin(), key.end(), new_entry.begin());
            std::copy(value.begin(), value.end(), new_entry.begin() + key.size());

            node.insert_entry(md, find_result, new_entry);
            node_info.btree_size++;
            node_info.is_found = true;
        }
        else
        {
            if (expect_leaf)
            {
                throw object_db_exception("unexpected branch node");
            }

            std::vector<uint8_t> new_node_entry(md.key_size + far_offset_ptr::get_size());
            std::copy(update_key.begin(), update_key.end(), new_node_entry.begin());
            std::span<uint8_t> new_value_span {
                new_node_entry.begin() + md.key_size,
                new_node_entry.end()
            };

            auto new_entry_span_it = span_iterator{ new_value_span };
            new_or_current_node_offset.write(new_entry_span_it);

            node.update_entry(md, find_result, new_node_entry);

            if (insert_needed)
            {
                std::vector<uint8_t> new_entry(insert_key.size() + far_offset_ptr::get_size());
                std::copy(insert_key.begin(), insert_key.end(), new_entry.begin());

                span_iterator value_it({ new_entry.begin() + insert_key.size(), new_entry.size() - insert_key.size() });
                insert_offset.write(value_it);
                auto find_result_insert = btree_node::find_result
                {
                    .position = find_result.position + 1, // Insert after the current position
                    .found = false // We are inserting a new key
                };
                node.insert_entry(md, find_result_insert, new_entry);
                node_info.btree_size++;
                node_info.is_found = true;
            }
        }

        new_or_current_node_offset = node_info.node_offset;

        if (node.get_transaction_id() != transaction_id)
        {
            new_or_current_node_offset = allocator.allocate_block(transaction_id);
            node.set_transaction_id(transaction_id);
        }

        btree_node insert_node;

        if (node.should_split())
        {
            insert_needed = true;

            node.split(insert_node);
            insert_node.set_transaction_id(transaction_id);
        }
        else
        {
            insert_needed = false;
        }

        auto write_it = cache.get_iterator(new_or_current_node_offset.get_file_id(), new_or_current_node_offset.get_offset());
        node.write(write_it);

        auto node_update_key_it = node.get_key_at(0);
        update_key = std::vector<uint8_t>(node_update_key_it.begin(), node_update_key_it.end());

        result_btree_position = 0;
        if (insert_needed)
        {
            auto new_node_offset = allocator.allocate_block(transaction_id);
            insert_offset = new_node_offset;
            auto insert_write_it = cache.get_iterator(new_node_offset.get_file_id(), new_node_offset.get_offset());
            insert_node.write(insert_write_it);

            auto insert_key_span = insert_node.get_key_at(0);
            insert_key = std::vector<uint8_t>(insert_key_span.begin(), insert_key_span.end());

            auto ec = node.get_entry_count();
            if (find_result.position > ec)
            {
                result.path[path_position].node_offset = new_node_offset;
                result.path[path_position].btree_position = find_result.position - ec;
                result_btree_position = 1;
            }
            else
            {
                result.path[path_position].node_offset = new_or_current_node_offset;
                result.path[path_position].btree_position = find_result.position;
            }
        }
        else
        {
            result.path[path_position].node_offset = new_or_current_node_offset;
            result.path[path_position].btree_position = find_result.position;
        }
        result.path[path_position].is_found = true;
        expect_leaf = false;
    }

    if (insert_needed)
    {
        // we now need a new root
        btree_node new_root;
        new_root.init_root();
        new_root.set_transaction_id(transaction_id);
        new_root.set_key_size(key.size());
        new_root.set_value_size(far_offset_ptr::get_size());
        auto md = new_root.get_metadata();
        auto fr = btree_node::find_result{
            .position = 0,
            .found = true
        };

        std::vector<uint8_t> first_entry(key.size() + far_offset_ptr::get_size());
        span_iterator span_it{ first_entry };
        write_span(span_it, update_key);
        new_or_current_node_offset.write(span_it);
        new_root.insert_entry(md, fr, first_entry);


        md = new_root.get_metadata();
        fr = btree_node::find_result{
            .position = 1,
            .found = true
        };
        std::vector<uint8_t> second_entry(key.size() + far_offset_ptr::get_size());
        span_iterator second_span_it{ second_entry };
        write_span(second_span_it, insert_key);
        insert_offset.write(second_span_it);

        new_root.insert_entry(md, fr, second_entry);

        auto tmp = result.path;

        auto new_root_offset = allocator.allocate_block(transaction_id);
        auto new_node_iterator = cache.get_iterator(new_root_offset.get_file_id(), new_root_offset.get_offset());

        new_root.write(new_node_iterator);

        btree_node_info new_node_info
        {
            .node_offset = new_root_offset,
            .btree_position = result_btree_position, // no - this will depend where the node was inserted
            .btree_size = 2,
            .key = update_key,
            .is_found = 1,
            .is_leaf = false,
            .is_root = true,
        };

        result.path.clear();
        result.path.push_back(new_node_info);
        for (auto t : tmp)
        {
            result.path.push_back(t);
        }
        result.btree_offset = new_root_offset;
    }

    return result;
}
btree_iterator btree_operations::update(filesize_t transaction_id, file_allocator& allocator, file_cache& cache, btree_iterator it, std::span<uint8_t> key, std::span<uint8_t> value)
{
    if (it.is_end())
    {
        throw object_db_exception("cannot update past end of index");
    }

    btree_iterator original = it;
    btree_iterator result = it;

    far_offset_ptr new_or_current_node_offset; 

    if (original.path.empty())
    {
        throw object_db_exception("Iterator path is empty, cannot update entry.");
    }

    bool expect_leaf = true;
    result = original;
    auto current_path = original.path;
    while (!current_path.empty())
    {
        auto node_info = current_path.back();
        if (!node_info.is_found)
        {
            throw object_db_exception("Key not found in the B-tree, cannot update entry.");
        }

        auto offset = node_info.node_offset;
        current_path.pop_back();
        int path_position = current_path.size();

        btree_node node;
        auto iterator = cache.get_iterator(offset.get_file_id(), offset.get_offset());
        node.read(iterator);
        auto md = node.get_metadata();
        auto find_result = node_info.get_find_result();

        if (node.is_leaf())
        {
            if (!expect_leaf)
            {
                throw object_db_exception("unexpected leaf node");
            }
            // Update the entry in the current node
            std::vector<uint8_t> new_entry(node_info.key.size() + value.size());
            std::copy(node_info.key.begin(), node_info.key.end(), new_entry.begin());
            std::copy(value.begin(), value.end(), new_entry.begin() + node_info.key.size());
            node.update_entry(md, find_result, new_entry);
        }
        else
        {
            if (expect_leaf)
            {
                throw object_db_exception("unexpected branch node");
            }

            std::vector<uint8_t> new_entry(node_info.key.size() + far_offset_ptr::get_size());
            std::copy(node_info.key.begin(), node_info.key.end(), new_entry.begin());
            
            span_iterator value_it({ new_entry.begin() + node_info.key.size(), new_entry.size() - node_info.key.size() });
            new_or_current_node_offset.write(value_it);
            node.update_entry(md, find_result, new_entry);
        }

        if (node.get_transaction_id() != transaction_id)
        {
            new_or_current_node_offset = allocator.allocate_block(transaction_id);
            node.set_transaction_id(transaction_id);
        }
        else
        {
            new_or_current_node_offset = offset;
        }

        auto write_it = cache.get_iterator(new_or_current_node_offset.get_file_id(), new_or_current_node_offset.get_offset());
        node.write(write_it);

        new_or_current_node_offset = offset;
        result.path[path_position].node_offset = new_or_current_node_offset;
        expect_leaf = false;
    }
    return result;
}

btree_iterator btree_operations::remove(filesize_t transaction_id, file_allocator& allocator, file_cache& cache, btree_iterator it)
{
    if (it.is_end())
    {
        throw object_db_exception("cannot remove past end of index");
    }

    auto original = it;
    auto result = it;
    auto current_path = original.path;

    if (current_path.empty())
    {
        btree_iterator iterator;
        return iterator;
    }

    far_offset_ptr offset;

    bool remove_needed = true;
    auto remove_position = current_path.back().btree_position;

    std::shared_ptr<btree_node> node;

    bool update_needed = false;
    uint32_t update_position = 0;
    std::vector<uint8_t> update_key;
    far_offset_ptr update_offset;

    while (!current_path.empty() && (remove_needed || update_needed))
    {
        auto info_node = current_path.back();
        if (!info_node.is_found)
        {
            throw object_db_exception("cannot remove a value unless it was found");
        }
        current_path.pop_back();
        int path_position = current_path.size();
        offset = info_node.node_offset;
        
        if (!node)
        {
            auto it = cache.get_iterator(info_node.node_offset);
            node = std::make_shared<btree_node>();
            node->read(it);
        }
        auto md = node->get_metadata();

        auto node_entry_count = md.entry_count;
        auto node_removed_at = remove_position;
        auto node_removed = remove_needed;
        if (remove_needed)
        {
            btree_node::find_result fr
            {
                .position = remove_position,
                .found = true
            };

            if (node_entry_count > remove_position)
            {
                node->remove_key(md, fr);
                node_entry_count--;
            }
            remove_needed = false;
        }

        md = node->get_metadata();

        if (update_needed)
        {
            std::vector<uint8_t> entry(md.key_size + far_offset_ptr::get_size());
            std::copy(update_key.begin(), update_key.end(), entry.begin());

            auto spit = span_iterator{entry, md.key_size};

            update_offset.write(spit);

            btree_node::find_result update_fr {
                .position = update_position,
                .found = true
            };

            node->update_entry(md, update_fr, entry);
        }
        update_needed = true; // we expect an update at every loop thereafter

        md = node->get_metadata();

        std::shared_ptr<btree_node> parent_node;
        std::shared_ptr<btree_node> other_node;

        far_offset_ptr other_node_offset;

        update_position = 0;
        remove_position = 0;

        if (path_position != 0 && node_removed)
        {
            auto parent_info_node = current_path.back();
            int node_position_in_parent = parent_info_node.btree_position;
            update_position = node_position_in_parent;

            
            if (node->should_merge())
            {
                // locate parent node
                auto parent_it = cache.get_iterator(parent_info_node.node_offset);
                parent_node = std::make_shared<btree_node>();
                parent_node->read(parent_it);

                auto parent_metadata = parent_node->get_metadata();

                int other_node_position = 1;
                if (parent_metadata.entry_count > 1)
                {
                    auto other_node_offset_span = parent_node->get_value_at(other_node_position);
                    auto other_node_offset_it = span_iterator{ other_node_offset_span };
                    other_node_offset.read(other_node_offset_it);

                    auto other_node_it = cache.get_iterator(other_node_offset);

                    other_node = std::make_shared<btree_node>();
                    other_node->read(other_node_it);
                    auto other_node_metadata = other_node->get_metadata();

                    auto total_count = other_node_metadata.entry_count + node_entry_count;
                    int position_in_total = 0;

                    if (other_node_position > node_position_in_parent)
                    {
                        position_in_total = node_removed_at;
                        node->merge(*other_node);
                    }
                    else
                    {
                        position_in_total = other_node_metadata.entry_count + node_removed_at;
                        other_node->merge(*node);

                        auto tmp_position = node_position_in_parent;
                        node_position_in_parent = other_node_position;
                        other_node_position = tmp_position;

                        std::shared_ptr<btree_node> tmp = node;
                        node = other_node;
                        other_node = tmp;
                    }

                    if (node->should_split())
                    {
                        node->split(*other_node);
                        remove_position = 0;
                        remove_needed = false;

                        if (other_node->get_transaction_id() != transaction_id) // do we need to copy on write?
                        {
                            other_node_offset = allocator.allocate_block(transaction_id);
                        }

                        std::vector<uint8_t> other_node_parent_entry(parent_metadata.key_size + far_offset_ptr::get_size());
                        auto entry0_span = other_node->get_value_at(0);
                        std::copy(entry0_span.begin(), entry0_span.end(), other_node_parent_entry.begin());

                        span_iterator spit{ other_node_parent_entry, parent_metadata.key_size };
                        other_node_offset.write(spit);

                        auto other_node_position_fr = btree_node::find_result{
                            .position = (uint32_t)other_node_position,
                            .found = true
                        };

                        auto other_node_it = cache.get_iterator(other_node_offset);
                        other_node->write(other_node_it);
                        parent_node->update_entry(parent_metadata, other_node_position_fr, entry0_span);
                    }
                    else
                    {
                        other_node.reset();
                        remove_position = other_node_position;
                        remove_needed = true;
                    }
                }

                if (node->get_entry_count() == 0)
                {
                    return btree_iterator{}; // this btree is now empty
                }
                update_position = node_position_in_parent;
            }
        }

        if (node->get_transaction_id() != transaction_id)
        {
            offset = allocator.allocate_block(transaction_id);
            update_needed = true;
        }

        if (node->get_entry_count() > 0)
        {
            auto update_span = node->get_key_at(0);
            update_key = std::vector<uint8_t>(update_span.begin(), update_span.end());
            update_offset = offset;
        }

        auto write_it = cache.get_iterator(offset.get_file_id(), offset.get_offset());

        node->write(write_it);
        result.path[path_position].node_offset = offset;
        result.path[path_position].btree_position =
            node_removed
            ? node_removed_at
            : info_node.btree_position; // Keep the same position

        result.path[path_position].btree_size = node->get_entry_count();
        result.path[path_position].is_found = result.path[path_position].btree_position < result.path[path_position].btree_size;
        if (result.path[path_position].is_found)
        {
            auto node_key = node->get_key_at(result.path[path_position].btree_position);
            result.path[path_position].key = std::vector<uint8_t>(node_key.begin(), node_key.end());
        }
        else
        {
            result.path[path_position].key.clear();
        }

        result.path[path_position].is_full = node->is_full();

        auto count = node->get_entry_count();
        if ((count == 0) || (!node->is_leaf() && count == 1)) // we have a new root, above this node (or the tree is now empty)
        {
            std::vector<btree_node_info> result_path(result.path.begin() + path_position + 1, result.path.end());
            result.path = result_path;
            break;
        }

        node = parent_node;
        parent_node.reset();
    }

    return it; //todo: does it make sense to return an iterator? what should it point to?
}
