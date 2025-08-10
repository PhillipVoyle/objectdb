
#include "../include/btree.hpp"
#include "../include/span_iterator.hpp"
#include <cassert>

btree::btree(std::shared_ptr<btree_row_traits> row_traits, file_cache& cache, far_offset_ptr offset, file_allocator& allocator):
    cache_(cache),
    allocator_(allocator),
    offset_(offset),
    row_traits_(row_traits)
{
}

std::shared_ptr<btree_row_traits> btree::get_row_traits()
{
    return row_traits_;
}

int btree::compare_keys(std::span<uint8_t> k1, std::span<uint8_t> k2)
{
    return row_traits_->get_key_traits()->compare(k1, k2);
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
    btree_iterator result;
    btree_node node(*this);
    auto current_offset = offset_;
    for (;;)
    {
        auto iterator = cache_.get_iterator(current_offset);
        node.read(iterator);

        btree_node_info info;
        info.node_offset = current_offset;
        info.btree_position = 0;
        info.is_found = false;
        info.btree_size = node.get_entry_count();
        if (node.get_entry_count() > 0)
        {
            info.is_found = 1;
        }
        else
        {
            info.is_found = 0;
            return result; // empty btree
        }

        result.path.push_back(info);

        if (node.is_leaf())
        {
            break;
        }
        else
        {
            auto val = node.get_value_at(0);
            span_iterator value_it{ val };
            current_offset.read(value_it);
        }
    }
    return result;
}

btree_iterator btree::end()
{
    btree_iterator result;
    result.btree_offset = offset_;

    far_offset_ptr current_offset = offset_;

    for (;;)
    {
        btree_node node(*this);
        auto iterator = cache_.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
        node.read(iterator);
        btree_node_info info;
        info.node_offset = current_offset;
        info.btree_position = node.get_entry_count(); // Position after the last entry
        info.btree_size = node.get_entry_count();
        info.is_found = false; // End iterator does not point to a valid entry
        result.path.push_back(info);
        if (node.is_leaf())
        {
            break;
        }
        else if (info.btree_size > 1)
        {
            auto& back = result.path.back();

            back.btree_position--; // branch node references should be valid
            back.is_found = true;

            auto entry_count = node.get_entry_count();
            // Move to the rightmost child
            auto child_offset_span = node.get_value_at(info.btree_size - 1);
            auto it = span_iterator(child_offset_span);
            current_offset.read(it);
        }
        else
        {
            throw object_db_exception("degenerate node found while seeking end");
        }
    }

    return result;
}

btree_iterator btree::next(btree_iterator it)
{
    return internal_next(it);
}
btree_iterator btree::prev(btree_iterator it)
{
    assert(it.btree_offset == offset_); //"Iterator must be from the same B-tree instance.";
    return internal_prev(it);
}

std::vector<uint8_t> btree::get_entry(btree_iterator it)
{
    return internal_get_entry(it);
}

btree_iterator btree::insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry)
{
    auto result =  internal_insert(transaction_id, it, entry);
    if (result.path.empty())
    {
        throw object_db_exception("B-tree is empty or corrupted, cannot insert entry.");
    }
    offset_ = result.path.front().node_offset; // Update the offset to the new root if it was created
    result.btree_offset = offset_; // Ensure the btree_offset is updated to the current B-tree root
    return result;

}
btree_iterator btree::update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry)
{
    auto result = internal_update(transaction_id, it, entry);
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
    auto result = internal_remove(transaction_id, it);

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

btree_iterator btree::internal_next(btree_iterator it)
{
    if (offset_.get_file_id() == 0 && offset_.get_offset() == 0)
    {
        btree_iterator result{}; // Invalid B-tree offset
        result.btree_offset = offset_;
        return result;
    }
    btree_iterator result = it;
    result.btree_offset = offset_;

    // If iterator is at end, return end iterator
    if (result.is_end()) 
    {
        return end();
    }

    // find a node with a next
    auto current_path = it.path;
    while(!current_path.empty())
    {
        auto& info = current_path.back();
        if ((info.btree_position + 1) < info.btree_size)
        {
            info.btree_position = info.btree_position + 1;
            break;
        }
        else
        {
            current_path.pop_back();
        }
    }

    if (current_path.empty())
    {
        // If we reach here, there is no next entry (we were at the last entry)
        return end();
    }

    for (;;)
    {
        auto& info = current_path.back();
        auto node_iterator = cache_.get_iterator(info.node_offset);
        btree_node node(*this);
        node.read(node_iterator);
        info.is_found = true;
        info.btree_size = node.get_entry_count();

        if (node.is_leaf())
        {
            break;
        }
        else
        {
            auto value_span = node.get_value_at(info.btree_position);
            auto span_it = span_iterator(value_span);
            far_offset_ptr next_node;
            next_node.read(span_it);
            btree_node_info new_info;
            new_info.btree_position = 0;
            new_info.node_offset = next_node;
            new_info.is_found = true;
            current_path.push_back(new_info);
        }
    }
    result.path = current_path;
    return result;
}
btree_iterator btree::internal_prev( btree_iterator it)
{
    if (offset_.get_file_id() == 0 && offset_.get_offset() == 0)
    {
        btree_iterator result{}; // Invalid B-tree offset
        result.btree_offset = offset_;
        return result;
    }
    btree_iterator result = it;
    result.btree_offset = offset_;

    // find a shared root
    auto current_path = it.path;
    while (!current_path.empty())
    {
        auto& info = current_path.back();

        if (info.btree_position == 0)
        {
            current_path.pop_back();
        }
        else
        {
            break;
        }
    }

    if (current_path.empty())
    {
        //todo: is this an error? basically means there is no earlier record (or is this fine?)
        return begin();
    }
    else
    {
        bool first = true;
        for (;;)
        {
            auto& info = current_path.back();
            auto it = cache_.get_iterator(info.node_offset);
            btree_node node(*this);
            node.read(it);

            auto node_size = node.get_entry_count();
            info.btree_size = node_size;
            info.is_found = true;

            if (first)
            {
                info.btree_position -= 1;
                first = false;
            }
            else
            {
                assert(node_size > 0);
                info.btree_position = node_size - 1;
            }

            if (node.is_leaf())
            {
                result.path = current_path;
                return result;
            }
            else
            {
                auto value = node.get_value_at(info.btree_position);
                span_iterator offset_it{ value };
                far_offset_ptr new_node_offset;
                new_node_offset.read(offset_it);

                btree_node_info new_node_info;
                new_node_info.node_offset = new_node_offset;
                current_path.push_back(new_node_info);
            }
        }
    }
}


std::vector<uint8_t> btree::internal_get_entry(btree_iterator it)
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
        btree_node node(*this);
        auto iterator = cache_.get_iterator(it.path.back().node_offset.get_file_id(), it.path.back().node_offset.get_offset());
        node.read(iterator);
        auto entry = node.get_entry(it.path.back().btree_position);
        return std::vector<uint8_t>(entry.begin(), entry.end());
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

btree_iterator btree::internal_insert(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry)
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
        btree_node node(*this);
        node.init_leaf();
        node.set_transaction_id(transaction_id);
        node.set_key_size(get_key_size());
        node.set_value_size(get_value_size());

        node.insert_leaf_entry(0, entry);
        node.set_transaction_id(transaction_id);
        new_or_current_node_offset = allocator_.allocate_block(transaction_id);
        auto write_it = cache_.get_iterator(new_or_current_node_offset.get_file_id(), new_or_current_node_offset.get_offset());
        node.write(write_it);
        btree_node_info info;
        info.node_offset = new_or_current_node_offset;
        info.btree_position = 0; // We inserted at the beginning
        info.is_found = true; // We found the entry we just inserted
        info.btree_size = node.get_entry_count();
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
        btree_node node(*this);
        auto iterator = cache_.get_iterator(
            node_info.node_offset.get_file_id(),
            node_info.node_offset.get_offset());

        node.read(iterator);
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

            node.insert_leaf_entry(find_result.position, entry);
            node_info.btree_size++;
            node_info.is_found = true;
        }
        else
        {
            if (expect_leaf)
            {
                throw object_db_exception("unexpected branch node");
            }

            node.update_branch_entry(find_result.position, update_key, new_or_current_node_offset);

            if (insert_needed)
            {
                node.insert_branch_entry(find_result.position + 1, insert_key, insert_offset);
                node_info.btree_size++;
                node_info.is_found = true;
            }
        }

        new_or_current_node_offset = node_info.node_offset;

        if (node.get_transaction_id() != transaction_id)
        {
            new_or_current_node_offset = allocator_.allocate_block(transaction_id);
            node.set_transaction_id(transaction_id);
        }

        btree_node insert_node(*this);

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

        auto write_it = cache_.get_iterator(new_or_current_node_offset.get_file_id(), new_or_current_node_offset.get_offset());
        node.write(write_it);

        update_key = node.get_key_at(0);

        result_btree_position = 0;
        if (insert_needed)
        {
            auto new_node_offset = allocator_.allocate_block(transaction_id);
            insert_offset = new_node_offset;
            auto insert_write_it = cache_.get_iterator(new_node_offset.get_file_id(), new_node_offset.get_offset());
            insert_node.write(insert_write_it);

            insert_key = insert_node.get_key_at(0);

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
        btree_node new_root(*this);
        new_root.init_root();
        new_root.set_transaction_id(transaction_id);
        new_root.set_key_size(get_key_size());
        new_root.set_value_size(far_offset_ptr::get_size());

        new_root.insert_branch_entry(0, update_key, new_or_current_node_offset);
        new_root.insert_branch_entry(1, insert_key, insert_offset);

        auto tmp = result.path;

        auto new_root_offset = allocator_.allocate_block(transaction_id);
        auto new_node_iterator = cache_.get_iterator(new_root_offset.get_file_id(), new_root_offset.get_offset());

        new_root.write(new_node_iterator);

        btree_node_info new_node_info
        {
            .node_offset = new_root_offset,
            .btree_position = result_btree_position, // no - this will depend where the node was inserted
            .btree_size = 2,
            .is_found = 1
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
btree_iterator btree::internal_update(filesize_t transaction_id, btree_iterator it, std::span<uint8_t> entry)
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

    std::vector<uint8_t> update_key = derive_key_from_entry(entry);
    
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

        btree_node node(*this);
        auto iterator = cache_.get_iterator(offset.get_file_id(), offset.get_offset());
        node.read(iterator);
        auto find_result = node_info.get_find_result();

        if (node.is_leaf())
        {
            if (!expect_leaf)
            {
                throw object_db_exception("unexpected leaf node");
            }

            // Update the entry in the current node
            node.update_leaf_entry(find_result.position, entry);
        }
        else
        {
            if (expect_leaf)
            {
                throw object_db_exception("unexpected branch node");
            }

            node.update_branch_entry(find_result.position, update_key, new_or_current_node_offset);
        }


        if (node.get_transaction_id() != transaction_id)
        {
            new_or_current_node_offset = allocator_.allocate_block(transaction_id);
            node.set_transaction_id(transaction_id);
        }
        else
        {
            new_or_current_node_offset = offset;
        }

        auto write_it = cache_.get_iterator(new_or_current_node_offset.get_file_id(), new_or_current_node_offset.get_offset());
        node.write(write_it);

        update_key = node.get_key_at(0);
        new_or_current_node_offset = offset;
        result.path[path_position].node_offset = new_or_current_node_offset;
        expect_leaf = false;
    }
    return result;
}

btree_iterator btree::internal_remove(filesize_t transaction_id, btree_iterator it)
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
            auto it = cache_.get_iterator(info_node.node_offset);
            node = std::make_shared<btree_node>(*this);
            node->read(it);
        }
        auto node_entry_count = node->get_entry_count();
        auto node_removed_at = remove_position;
        auto node_removed = remove_needed;
        if (remove_needed)
        {

            if (node_entry_count > remove_position)
            {
                node->remove_key(remove_position);
                node_entry_count--;
            }
            remove_needed = false;
        }


        if (update_needed)
        {
            node->update_branch_entry(update_position, update_key, update_offset);
        }
        update_needed = true; // we expect an update at every loop thereafter

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
                auto parent_it = cache_.get_iterator(parent_info_node.node_offset);
                parent_node = std::make_shared<btree_node>(*this);
                parent_node->read(parent_it);


                int other_node_position = 1;
                if (parent_node->get_entry_count() > 1)
                {
                    auto other_node_offset_span = parent_node->get_value_at(other_node_position);
                    auto other_node_offset_it = span_iterator{ other_node_offset_span };
                    other_node_offset.read(other_node_offset_it);

                    auto other_node_it = cache_.get_iterator(other_node_offset);

                    other_node = std::make_shared<btree_node>(*this);
                    other_node->read(other_node_it);

                    auto other_node_entry_count = other_node->get_entry_count();

                    int position_in_total = 0;

                    if (other_node_position > node_position_in_parent)
                    {
                        position_in_total = node_removed_at;
                        node->merge(*other_node);
                    }
                    else
                    {
                        position_in_total = other_node_entry_count + node_removed_at;
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
                            other_node_offset = allocator_.allocate_block(transaction_id);
                        }

                        auto tmp_other_node_it = cache_.get_iterator(other_node_offset);
                        other_node->write(tmp_other_node_it);

                        auto key_at_0 = other_node->get_key_at(0);
                        parent_node->update_branch_entry(other_node_position, key_at_0, other_node_offset);
                    }
                    else
                    {
                        other_node.reset();
                        remove_position = (uint16_t)other_node_position;
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
            offset = allocator_.allocate_block(transaction_id);
            update_needed = true;
        }

        if (node->get_entry_count() > 0)
        {
            update_key = node->get_key_at(0);
            update_offset = offset;
        }

        auto write_it = cache_.get_iterator(offset.get_file_id(), offset.get_offset());

        node->write(write_it);
        result.path[path_position].node_offset = offset;
        result.path[path_position].btree_position =
            node_removed
            ? node_removed_at
            : info_node.btree_position; // Keep the same position

        result.path[path_position].btree_size = node->get_entry_count();
        result.path[path_position].is_found = result.path[path_position].btree_position < result.path[path_position].btree_size;

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

    return it;
}

btree_iterator btree::seek_begin(std::span<uint8_t> key) // seek to the first entry that is greater than or equal to the key
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
        auto find_result = node.find_key(key);
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

btree_iterator btree::seek_end(std::span<uint8_t> key) // seek to the first entry that is greater than the key
{
    if (offset_.get_file_id() == 0 && offset_.get_offset() == 0)
    {

        btree_iterator result{}; // Invalid B-tree offset
        result.btree_offset = offset_;
        return result;
    }

    auto it = seek_begin(key);
    if (it.is_end())
    {
        return it; // If the key is not found, return end iterator
    }
    // Move to the next entry if the key is found
    if (it.path.back().is_found)
    {
        // Move to the next entry in the B-tree
        it = next(it);
    }
    else {
        // If the key was not found, we return the iterator at the position where it would be inserted
        // This is already handled by seek_begin, so we just return it
    }

    it.btree_offset = offset_;
    return it;
}

btree_iterator btree::upsert(filesize_t transaction_id, std::span<uint8_t> entry) // insert or update an entry in the B-tree
{
    auto key = derive_key_from_entry(entry);
    btree_iterator it = seek_begin(key);
    if (it.is_end() || !it.path.back().is_found)
    {
        // If the key is not found, we need to insert it
        return insert(transaction_id, it, entry);
    }
    else
    {
        // If the key is found, we can update it
        return update(transaction_id, it, entry);
    }
}


std::vector<uint8_t> btree::derive_key_from_entry(std::span<uint8_t> entry)
{
    auto key_traits = row_traits_->get_key_traits();
    return key_traits->get_data(entry);
}

uint32_t btree::get_value_size()
{
    auto value_traits = row_traits_->get_value_traits();
    return value_traits->get_size();
}

uint32_t btree::get_key_size()
{
    auto key_traits = row_traits_->get_key_traits();
    return key_traits->get_size();
}
