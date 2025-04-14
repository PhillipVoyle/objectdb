#include <algorithm>
#include <iterator>
#include "../include/logging_btree.hpp"

variable_btree_node::variable_btree_node(logging_btree& btree) :_btree(btree)
{
}

filesize_t variable_btree_node::get_offset() const
{
    return _offset;
}


random_access_file& variable_btree_node::file()
{
    return _btree._file;
}

bool variable_btree_node::read_node(filesize_t offset)
{
    _offset = offset;
    auto current_offset = offset;
    uint32_t flags = 0;
    if (!read_uint32(file(), current_offset, flags))
    {
        return false;
    }

    _is_leaf = flags == 1;
    current_offset += 4;

    if (!read_uint32(file(), current_offset, _key_size))
    {
        return false;
    }
    current_offset += 4;

    if (!read_uint32(file(), current_offset, _value_size))
    {
        return false;
    }

    current_offset += 4;

    uint32_t value_count = 0;
    if (!read_uint32(file(), current_offset, value_count))
    {
        return false;
    }
    current_offset += 4;

    auto data_size = (_value_size + _key_size) * value_count;
    _data.resize(data_size);
    _data.shrink_to_fit();

    std::span<uint8_t> s(_data);
    return file().read_data(current_offset, s);
}

int variable_btree_node::get_entry_size() const
{
    return (_value_size + _key_size);
}

int variable_btree_node::get_value_count() const
{
    uint32_t value_count = _data.size() / get_entry_size();
    return value_count;
}

bool variable_btree_node::write_node(filesize_t offset)
{
    _offset = offset; // todo: is this right?
    auto current_offset = offset;
    uint32_t flags = 0;
    if (_is_leaf)
    {
        flags = 1;
    }
    if (!write_uint32(file(), current_offset, flags))
    {
        return false;
    }

    current_offset += 4;

    if (!write_uint32(file(), current_offset, _key_size))
    {
        return false;
    }
    current_offset += 4;

    if (!write_uint32(file(), current_offset, _value_size))
    {
        return false;
    }

    current_offset += 4;

    uint32_t value_count = get_value_count();
    if (!write_uint32(file(), current_offset, value_count))
    {
        return false;
    }

    current_offset += 4;

    std::span<uint8_t> s(_data);
    return file().write_data(current_offset, s);
}

filesize_t variable_btree_node::get_node_size() const
{
    return 16 + _data.size();
}

bool variable_btree_node::get_key_at_n(int n, std::span<const uint8_t>& key) const
{
    auto data_offset = get_entry_size() * n;
    auto value_offset = data_offset + _key_size;

    assert(value_offset <= _data.size());

    key = std::span<const uint8_t>(_data.begin() + data_offset, _data.begin() + value_offset);

    return true;
}

bool variable_btree_node::get_value_at_n(int n, std::span<const uint8_t>& value) const
{
    auto data_offset = get_entry_size() * n;
    auto value_offset = data_offset + _key_size;
    auto value_end_offset = value_offset + _value_size;

    assert(value_end_offset <= _data.size());

    value = std::span<const uint8_t>(_data.begin() + value_offset, _data.begin() + value_end_offset);
    return true;
}

int compare_span(std::span<const uint8_t> a, std::span<const uint8_t> b)
{
    auto ita = a.begin();
    auto itb = b.begin();

    for (;;)
    {
        if (ita == a.end())
        {
            if (itb == b.end())
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
        else if (itb == b.end())
        {
            return 1;
        }
        else if (*ita < *itb)
        {
            return -1;
        }
        else if (*itb < *ita)
        {
            return 1;
        }
        else// if (*itb == *ita)
        {
            ita++;
            itb++;
        }
    }
}

bool logging_btree::internal_find_key(variable_btree_node& node, const std::span<uint8_t>& key, value_location& location)
{
    auto data_count = node.get_value_count();
    int prev_position = 0;
    int comparison = -1;
    std::span<const uint8_t> span_copy = key;

    for (int n = 0; n < data_count; n++)
    {
        std::span<const uint8_t> key_at_n;
        if (!node.get_key_at_n(n, key_at_n))
        {
            return false;
        }
        comparison = compare_span(span_copy, key_at_n);
        if (comparison < 0)
        {
            break;
        }
        prev_position = n;
        if (comparison == 0)
        {
            break;
        }
    }

    key_offset offset;
    offset.value_offset = prev_position;
    offset.comparison = comparison;
    offset.node_offset = node.get_offset();

    location.best_value_position.push_back(offset);
    if (!node._is_leaf)
    {
        std::span<const uint8_t> value_at_position;
        if (!node.get_value_at_n(prev_position, value_at_position))
        {
            return false;
        }
        if (value_at_position.size() == 8)
        {
            auto it = value_at_position.begin();

            filesize_t next_offset = 0;
            next_offset = *it ++;
            next_offset <<= 8;
            next_offset |= *it++;
            next_offset <<= 8;
            next_offset |= *it++;
            next_offset <<= 8;
            next_offset |= *it++;

            next_offset <<= 8;
            next_offset = *it++;
            next_offset <<= 8;
            next_offset |= *it++;
            next_offset <<= 8;
            next_offset |= *it++;
            next_offset <<= 8;
            next_offset |= *it;

            node.read_node(next_offset);
            return internal_find_key(node, key, location);
        }
    }
    return true;
}

logging_btree::logging_btree(random_access_file& file) : _file(file)
{
}

bool logging_btree::create_empty_root_node(filesize_t offset, filesize_t& node_size)
{
    variable_btree_node node(*this);
    node._is_leaf = true;
    node._key_size = key_size;
    node._value_size = sizeof(filesize_t);
    node._data.resize(0);
    node._data.shrink_to_fit();
    if (!node.write_node(offset))
    {
        return false;
    }

    node_size = node.get_node_size();
    return true;
}

bool logging_btree::find_key(int root_offset, const std::span<uint8_t>& key, value_location& location)
{
    variable_btree_node node(*this);
    node.read_node(root_offset);

    return internal_find_key(node, key, location);
}

bool logging_btree::insert_key_and_data(const value_location& location, const std::span<uint8_t>& key, const std::span<uint8_t>& data) {
    if (location.best_value_position.empty()) return false;

    // Start recursive insert
    bool root_split = false;
    variable_btree_node new_root(*this);
    if (insert_recursive(location.best_value_position.back().node_offset, key, data, new_root, root_split)) {
        if (root_split) {
            // Root split, need to create a new root
            filesize_t new_root_offset = 0;
            create_empty_root_node(0, new_root_offset);
            new_root.write_node(new_root_offset);
        }
        return true;
    }
    return false;
}

bool logging_btree::insert_recursive(filesize_t node_offset, const std::span<uint8_t>& key, const std::span<uint8_t>& data, variable_btree_node& parent_node, bool& root_split) {
    variable_btree_node current_node(*this);
    if (!current_node.read_node(node_offset)) return false;

    // Handle internal or leaf nodes
    int count = current_node.get_value_count();
    bool is_leaf = current_node._is_leaf;

    // If the node is a leaf, insert directly
    if (is_leaf) {
        // Copy node and insert the key/data
        std::vector<uint8_t> new_entry(current_node.get_entry_size());
        std::memcpy(new_entry.data(), key.data(), key.size());
        std::memcpy(new_entry.data() + key.size(), data.data(), data.size());

        std::copy(new_entry.begin(), new_entry.end(), std::back_inserter<std::vector<uint8_t>>(current_node._data));

        // If it exceeds the max allowed entries, split
        if (current_node.get_value_count() > get_maximum_value_count()) {
            return split_node(current_node, parent_node, root_split);
        }
        else {
            return current_node.write_node(node_offset);
        }
    }
    else {
        // Internal node, find the correct child node
        int insert_pos = 0;
        while (insert_pos < count && compare_span(key, { current_node._data.data() + insert_pos * current_node._key_size, current_node._key_size }) > 0) {
            insert_pos++;
        }

        // Recursively insert
        // 
        // in the correct child
        bool child_split = false;
        bool result = insert_recursive(current_node._data[insert_pos], key, data, current_node, child_split);

        if (child_split) {
            // If child split occurred, we need to insert the new key into the parent node
            std::vector<uint8_t> new_entry(current_node.get_entry_size());
            std::memcpy(new_entry.data(), key.data(), key.size());
            std::memcpy(new_entry.data() + key.size(), data.data(), data.size());

            std::copy(new_entry.begin(), new_entry.end(), std::back_inserter<std::vector<uint8_t>>(current_node._data));

            if (current_node.get_value_count() > get_maximum_value_count()) {
                return split_node(current_node, parent_node, root_split);
            }
            else {
                return current_node.write_node(node_offset);
            }
        }
        return result;
    }
}

bool logging_btree::split_node(variable_btree_node& node, variable_btree_node& parent_node, bool& root_split) {
    // Split the node into two halves and promote middle key
    int mid_index = node.get_value_count() / 2;
    std::vector<uint8_t> middle_key(node._key_size + value_size);
    int mid_offset = mid_index * (node._key_size + node._value_size);
    std::memcpy(middle_key.data(), node._data.data() + mid_offset, node._key_size + value_size);

    // Create a new right-hand node
    variable_btree_node new_node(*this);
    new_node._is_leaf = node._is_leaf;
    new_node._key_size = node._key_size;
    new_node._value_size = node._value_size;
    new_node._data.assign(node._data.begin() + mid_offset, node._data.end());

    // Remove the right half from the current node
    node._data.resize(mid_index);

    // Write both nodes back
    filesize_t new_node_offset = 0;
    new_node.write_node(new_node_offset);
    node.write_node(node.get_offset());

    // Promote the middle key to the parent
    if (parent_node._data.size() < get_maximum_value_count()) {
        std::copy(middle_key.begin(), middle_key.end(), std::back_inserter<std::vector<uint8_t>>(parent_node._data));
        return parent_node.write_node(parent_node.get_offset());
    }
    else {
        return split_node(parent_node, parent_node, root_split);
    }
}

