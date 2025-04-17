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
    if (!file().write_data(current_offset, s))
    {
        return false;
    }

    if (!_btree.copy_on_write)
    {
        // if we're not writing a new node each time the node needs to have a predictable size
        // fill in the remaining bytes with 0
        auto remaining_count = (_btree.get_maximum_value_count() - get_value_count());
        assert(remaining_count >= 0);
        if (remaining_count > 0)
        {
            std::vector<uint8_t> zero_fill(remaining_count * get_entry_size(), 0);
            std::span<uint8_t> zero_span(zero_fill);
            if (!file().write_data(current_offset + s.size(), zero_span))
            {
                return false;
            }
        }
    }

    return true;
}

filesize_t variable_btree_node::get_node_size() const
{
    if (_btree.copy_on_write)
    {
        // if we're using a copy on write, we can just return the size of the 
        return 16 + _data.size();
    }
    else
    {
        // if we aren't copying on write, we may need to grow the node
        // which would mean overlaps. We don't want that, so instead calculate a predictable size
        // for the node.
        // this is the size of the header
        auto header_size = 4 + 4 + 4 + 4;
        // this is the size of the data
        // note this size may depend on whether the node is a leaf or internal node
        auto data_size = _btree.get_maximum_value_count() * get_entry_size();
        return header_size + data_size;
    }
}

bool variable_btree_node::get_key_at_n(int n, std::span<uint8_t>& key) const
{
    auto data_offset = get_entry_size() * n;
    auto value_offset = data_offset + _key_size;

    assert(value_offset <= _data.size());
    assert(key.size() == _key_size);

    std::copy(_data.begin() + data_offset, _data.begin() + value_offset, key.begin());

    return true;
}

bool variable_btree_node::get_value_at_n(int n, std::span<uint8_t>& value) const
{
    auto data_offset = get_entry_size() * n;
    auto value_offset = data_offset + _key_size;
    auto value_end_offset = value_offset + _value_size;

    assert(value_end_offset <= _data.size());
    assert(value.size() == _value_size);

    std::copy(_data.begin() + value_offset, _data.begin() + value_end_offset, value.begin());
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
        std::span<uint8_t> key_at_n;
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
        std::span<uint8_t> value_at_position;
        if (!node.get_value_at_n(prev_position, value_at_position))
        {
            return false;
        }
        if (value_at_position.size() == 8)
        {

            auto it = value_at_position.begin();

            filesize_t next_offset = 0;
            if (!read_filesize(value_at_position, next_offset))
            {
                return 0;
            }

            node.read_node(next_offset);
            return internal_find_key(node, key, location);
        }
    }
    return true;
}

logging_btree::logging_btree(const logging_btree_parameters& parms) :
    _file(parms.file),
    key_size(parms.key_size),
    value_size(parms.value_size),
    maximum_value_count(parms.maximum_value_count),
    copy_on_write(parms.copy_on_write)
{
    assert(key_size > 0 && value_size > 0);
}

/// <summary>
/// this is the first node in the btree. Used only when creating a new btree.
/// </summary>
bool logging_btree::create_empty_root_node(filesize_t offset, filesize_t& node_size)
{
    variable_btree_node node(*this);
    node._is_leaf = true;
    node._key_size = key_size;
    node._value_size = value_size;
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
    if (!node.read_node(root_offset))
    {
        return false;
    }

    return internal_find_key(node, key, location);
}

bool logging_btree::insert_key_and_data(filesize_t root_offset, const std::span<uint8_t>& key, const std::span<uint8_t>& data, filesize_t& new_root_offset)
{
    // Start recursive insert
    bool root_split = false;
    std::vector<uint8_t> new_node_key;
    filesize_t new_node_offset = 0;
    std::vector<uint8_t> current_node_key_bytes(key_size);
    filesize_t current_node_offset = 0;
    new_root_offset = root_offset;

    if (!insert_recursive(root_offset, key, data, root_split, new_node_key, new_node_offset,current_node_key_bytes, current_node_offset))
    {
        return false;
    }

    // If the root was split, we need to create a new root node
    if (root_split)
    {
        variable_btree_node new_root(*this);
        new_root_offset = _file.get_file_size();
        new_root._is_leaf = false;
        new_root._key_size = key_size;
        new_root._value_size = sizeof(filesize_t);
        new_root._data.resize(0);

        std::vector<uint8_t> current_offset_bytes(sizeof(filesize_t));
        std::span<uint8_t> current_offset_span(current_offset_bytes);
        if (!write_filesize(current_offset_span, current_node_offset))
        {
            return false;
        }

        std::vector<uint8_t> new_offset_bytes(sizeof(filesize_t));
        std::span<uint8_t> new_offset_span{ new_offset_bytes };
        if (!write_filesize(new_offset_span, new_node_offset))
        {
            return false;
        }

        std::span<const uint8_t> current_node_key{ current_node_key_bytes };
        new_root._data.insert(new_root._data.end(), current_node_key.begin(), current_node_key.end());
        new_root._data.insert(new_root._data.end(), current_offset_bytes.begin(), current_offset_bytes.end());

        new_root._data.insert(new_root._data.end(), new_node_key.begin(), new_node_key.end());
        new_root._data.insert(new_root._data.end(), new_offset_bytes.begin(), new_offset_bytes.end());

        new_root._data.shrink_to_fit();

        // Write the new root node to the file
        if (!new_root.write_node(new_root_offset))
        {
            return false;
        }
    }
    return true;
}

bool logging_btree::insert_recursive(filesize_t node_offset, const std::span<uint8_t>& key, const std::span<uint8_t>& data, bool& node_is_split, std::vector<uint8_t>& new_node_key, filesize_t& new_node_offset, std::vector<uint8_t>& current_node_key, filesize_t& current_node_offset)
{
    node_is_split = false;
    variable_btree_node current_node(*this);
    if (!current_node.read_node(node_offset)) return false;
    current_node_offset = node_offset;

    int count = current_node.get_value_count();

    bool is_leaf = current_node._is_leaf;

    // find the insert position
    int insert_pos = 0;
    while (insert_pos < count && compare_span(key, { current_node._data.data() + insert_pos * current_node._key_size, current_node._key_size }) > 0)
    {
        insert_pos++;
    }

    std::vector<uint8_t> insert_value; // placeholder. Used only if this is a branch node
    std::span<uint8_t> insert_value_span;

    bool insert_required = false;
    bool split_required = false;

    if (is_leaf)
    {
        // if this is a leaf, yes an insert is required
        insert_required = true;
        insert_value_span = data;
    }
    else
    {
        // If not a leaf, we need to find the child node to insert into
        std::vector<uint8_t> value_at_position_bytes(current_node._value_size);
        std::span<uint8_t> value_at_position{ value_at_position_bytes };

        if (!current_node.get_value_at_n(insert_pos, value_at_position))
        {
            return false;
        }
        filesize_t next_offset = 0;
        if (!read_filesize(value_at_position, next_offset))
        {
            return false;
        }

        std::vector<uint8_t> key_at_position_bytes(current_node._key_size);
        std::span<uint8_t> key_at_position{ key_at_position_bytes };

        if (!current_node.get_key_at_n(insert_pos, key_at_position))
        {
            return false;
        }

        std::vector<uint8_t> child_node_key(current_node._key_size);
        filesize_t child_node_offset = 0;

        if (insert_recursive(next_offset, key, data, node_is_split, new_node_key, new_node_offset, child_node_key, child_node_offset))
        {
            if (node_is_split)
            {
                insert_required = true;
                insert_value.resize(sizeof(filesize_t));
                insert_value_span = { insert_value };
                write_filesize(insert_value_span, new_node_offset);
            }
        }
        std::span<const uint8_t>{ child_node_key };
        if (compare_span(child_node_key, key_at_position) != 0) 
        {
            // a key update is necessary
            current_node._data.insert(current_node._data.begin() + insert_pos * current_node.get_entry_size(), child_node_key.begin(), child_node_key.end());
        }
    }

    if (insert_required)
    {
        if (count >= get_maximum_value_count())
        {
            split_required = true;
        }
    }

    // if the node is a leaf then we know the node's value, and we can decide simply whether to split
    if (insert_required)
    {
        assert(current_node._key_size == key.size());
        assert(current_node._value_size == insert_value_span.size());

        if (split_required)
        {
            // Split the node
            variable_btree_node new_node(*this);
            int mid_index = count / 2;
            int mid_offset = mid_index * current_node.get_entry_size();

            // Build the new node data
            new_node._is_leaf = current_node._is_leaf;
            new_node._key_size = current_node._key_size;
            new_node._value_size = current_node._value_size;
            new_node._data.assign(current_node._data.begin() + mid_offset, current_node._data.end());


            // Set the new node key
            new_node_key.assign(current_node._data.begin() + mid_offset, current_node._data.begin() + mid_offset + current_node._key_size);

            // Adjust the current node data
            current_node._data.resize(mid_offset);

            // Insert the new key and data into the correct node
            if (insert_pos > mid_index)
            {
                insert_pos -= mid_index;
                new_node._data.insert(new_node._data.begin() + insert_pos * new_node.get_entry_size(), key.begin(), key.end());
                new_node._data.insert(new_node._data.begin() + insert_pos * new_node.get_entry_size() + new_node._key_size, insert_value_span.begin(), insert_value_span.end());
            }
            else
            {
                current_node._data.insert(current_node._data.begin() + insert_pos * current_node.get_entry_size(), key.begin(), key.end());
                current_node._data.insert(current_node._data.begin() + insert_pos * current_node.get_entry_size() + current_node._key_size, insert_value_span.begin(), insert_value_span.end());
            }

            // Write both nodes back
            new_node_offset = _file.get_file_size();
            if (!new_node.write_node(new_node_offset))
            {
                return false;
            }

            if (!current_node.write_node(node_offset))
            {
                return false;
            }

            node_is_split = true;
        }
        else
        {
            // Insert the key and data into the current node
            int offset = insert_pos * current_node.get_entry_size();
            current_node._data.insert(current_node._data.begin() + offset, key.begin(), key.end());
            current_node._data.insert(current_node._data.begin() + offset + current_node._key_size, insert_value_span.begin(), insert_value_span.end());
            return current_node.write_node(node_offset);
        }
    }

    // this may tell the caller that it needs to update it's key for the current node
    current_node_key.resize(current_node._key_size);
    std::span<uint8_t> current_node_span{ current_node_key };

    return current_node.get_key_at_n(0, current_node_span);
}


bool logging_btree::read_value_at_key(filesize_t node_offset, const std::span<uint8_t>& key, bool& found, std::vector<uint8_t>& data)
{
    variable_btree_node current_node(*this);
    if (!current_node.read_node(node_offset)) return false;

    found = false;
    int position = 0;
    int prev_position = -1;
    for (position = 0; position < current_node.get_value_count(); position++)
    {
        std::vector<uint8_t> key_at_n_bytes(current_node._key_size);
        std::span<uint8_t> key_at_n{ key_at_n_bytes };
        if (!current_node.get_key_at_n(position, key_at_n))
        {
            return false;
        }
        auto comparison = compare_span(key, key_at_n);

        if (comparison < 0)
        {
            break;
        }

        prev_position = position;
        if (comparison == 0)
        {
            found = true;
            break;
        }
    }

    if (current_node._is_leaf)
    {
        if (found)
        {
            std::vector<uint8_t> value_at_n_bytes(current_node._value_size);
            std::span<uint8_t> value_at_n{ value_at_n_bytes };
            if (!current_node.get_value_at_n(position, value_at_n))
            {
                return false;
            }
            data.assign(value_at_n.begin(), value_at_n.end());
        }
        return true;
    }
    else if (prev_position == -1)
    {
        found = false;
    }
    else
    {
        std::vector<uint8_t> value_at_n_bytes(current_node._value_size);
        std::span<uint8_t> value_at_n{ value_at_n_bytes };
        if (!current_node.get_value_at_n(prev_position, value_at_n))
        {
            return false;
        }
        filesize_t next_offset = 0;
        if (!read_filesize(value_at_n, next_offset))
        {
            return false;
        }
        return read_value_at_key(next_offset, key, found, data);
    }
    return true;
}