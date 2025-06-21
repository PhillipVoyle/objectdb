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

void variable_btree_node::read_node(filesize_t offset)
{
    _offset = offset;
    auto current_offset = offset;
    uint32_t flags = 0;
    read_uint32(file(), current_offset, flags);

    _is_leaf = flags == 1;
    current_offset += 4;

    read_uint32(file(), current_offset, _key_size);
    current_offset += 4;

    read_uint32(file(), current_offset, _value_size);

    current_offset += 4;

    uint32_t value_count = 0;
    read_uint32(file(), current_offset, value_count);
    current_offset += 4;

    auto data_size = (_value_size + _key_size) * value_count;
    _data.resize(data_size);
    _data.shrink_to_fit();

    file().read_data(current_offset, _data);
}

int variable_btree_node::get_entry_size() const
{
    return (_value_size + _key_size);
}

int variable_btree_node::get_value_count() const
{
    uint32_t value_count = static_cast<uint32_t>(_data.size() / get_entry_size());
    return value_count;
}

void variable_btree_node::write_node(filesize_t offset)
{
    _offset = offset; // todo: is this right?
    auto current_offset = offset;
    uint32_t flags = 0;
    if (_is_leaf)
    {
        flags = 1;
    }
    write_uint32(file(), current_offset, flags);

    current_offset += 4;

    write_uint32(file(), current_offset, _key_size);
    current_offset += 4;

    write_uint32(file(), current_offset, _value_size);

    current_offset += 4;

    uint32_t value_count = get_value_count();
    write_uint32(file(), current_offset, value_count);

    current_offset += 4;

    file().write_data(current_offset, _data);

    if (!_btree.copy_on_write)
    {
        // if we're not writing a new node each time the node needs to have a predictable size
        // fill in the remaining bytes with 0
        auto remaining_count = (_btree.get_maximum_value_count() - get_value_count());
        assert(remaining_count >= 0);
        if (remaining_count > 0)
        {
            std::vector<uint8_t> zero_fill(remaining_count * get_entry_size(), 0);
            file().write_data(current_offset + _data.size(), zero_fill);
        }
    }
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

void variable_btree_node::get_key_at_n(int n, const std::span<uint8_t>& key) const
{
    auto data_offset = get_entry_size() * n;
    auto value_offset = data_offset + _key_size;

    assert(value_offset <= _data.size());
    assert(key.size() == _key_size);

    std::copy(_data.begin() + data_offset, _data.begin() + value_offset, key.begin());
}

void variable_btree_node::get_value_at_n(int n, const std::span<uint8_t>& value) const
{
    auto data_offset = get_entry_size() * n;
    auto value_offset = data_offset + _key_size;
    auto value_end_offset = value_offset + _value_size;

    assert(value_end_offset <= _data.size());
    assert(value.size() == _value_size);

    std::copy(_data.begin() + value_offset, _data.begin() + value_end_offset, value.begin());
}



void logging_btree::internal_find_key(variable_btree_node& node, const std::span<uint8_t>& key, value_location& location)
{
    auto data_count = node.get_value_count();
    int prev_position = 0;
    int comparison = -1;

    for (int n = 0; n < data_count; n++)
    {
        std::vector<uint8_t> key_at_n;
        node.get_key_at_n(n, key_at_n);
        comparison = compare_span(key, key_at_n);
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
        std::vector<uint8_t> value_at_position(node._value_size);
        node.get_value_at_n(prev_position, value_at_position);

        if (value_at_position.size() == 8)
        {
            auto it = value_at_position.begin();

            filesize_t next_offset = 0;
            read_filesize(value_at_position, next_offset);

            node.read_node(next_offset);
            return internal_find_key(node, key, location);
        }
    }
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
void logging_btree::create_empty_root_node(filesize_t offset, filesize_t& node_size)
{
    variable_btree_node node(*this);
    node._is_leaf = true;
    node._key_size = key_size;
    node._value_size = value_size;
    node._data.resize(0);
    node._data.shrink_to_fit();
    node.write_node(offset);
    node_size = node.get_node_size();
}

void logging_btree::find_key(int root_offset, const std::span<uint8_t>& key, value_location& location)
{
    variable_btree_node node(*this);
    node.read_node(root_offset);
    internal_find_key(node, key, location);
}

void logging_btree::insert_key_and_data(filesize_t root_offset, const std::span<uint8_t>& key, const std::span<uint8_t>& data, filesize_t& new_root_offset)
{
    // Start recursive insert
    bool root_split = false;
    std::vector<uint8_t> new_node_key;
    filesize_t new_node_offset = 0;
    std::vector<uint8_t> current_node_key(key_size);
    filesize_t current_node_offset = 0;
    new_root_offset = root_offset;

    insert_recursive(root_offset, key, data, root_split, new_node_key, new_node_offset, current_node_key, current_node_offset);

    // If the root was split, we need to create a new root node
    if (root_split)
    {
        variable_btree_node new_root(*this);
        new_root_offset = _file.get_file_size();
        new_root._is_leaf = false;
        new_root._key_size = key_size;
        new_root._value_size = sizeof(filesize_t);
        new_root._data.resize(0);

        std::vector<uint8_t> current_offset(sizeof(filesize_t));
        write_filesize(current_offset, current_node_offset);

        std::vector<uint8_t> new_offset_bytes(sizeof(filesize_t));
        write_filesize(new_offset_bytes, new_node_offset);

        new_root._data.insert(new_root._data.end(), current_node_key.begin(), current_node_key.end());
        new_root._data.insert(new_root._data.end(), current_offset.begin(), current_offset.end());

        new_root._data.insert(new_root._data.end(), new_node_key.begin(), new_node_key.end());
        new_root._data.insert(new_root._data.end(), new_offset_bytes.begin(), new_offset_bytes.end());

        new_root._data.shrink_to_fit();

        // Write the new root node to the file
        new_root.write_node(new_root_offset);
    }
}

void logging_btree::insert_recursive(filesize_t node_offset, const std::span<uint8_t>& key, const std::span<uint8_t>& data, bool& node_is_split, std::vector<uint8_t>& new_node_key, filesize_t& new_node_offset, std::vector<uint8_t>& current_node_key, filesize_t& current_node_offset)
{
    node_is_split = false;
    variable_btree_node current_node(*this);
    current_node.read_node(node_offset);
    current_node_offset = node_offset;

    int count = current_node.get_value_count();

    bool is_leaf = current_node._is_leaf;

    // find the insert position
    int insert_pos = 0;
    for(;;)
    {
        if (insert_pos >= count)
        {
            break;
        }
        std::vector<uint8_t> key_at_position(current_node._key_size);
        current_node.get_key_at_n(insert_pos, key_at_position);
        int comparison = compare_span(key, key_at_position);

        if (comparison < 0)
        {
            break;
        }
        else if (comparison == 0)
        {
            // key already exists, we need to update the value
            if (is_leaf)
            {
                throw object_db_exception("key already exists");
            }
        }
        insert_pos++;
    }

    std::vector<uint8_t> insert_value; // placeholder. Used only if this is a branch node
    std::span<uint8_t> insert_value_span;
    std::vector<uint8_t> insert_key;
    std::span<uint8_t> insert_key_span = key;

    bool insert_required = false;
    bool split_required = false;
    bool updated_occurred = false;

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

        // the only way we expand a node is by splitting a child node, so we don't want to go off the end
        auto get_value_pos = insert_pos;
        if (get_value_pos >= count)
        {
            get_value_pos = count - 1;
        }

        current_node.get_value_at_n(get_value_pos, value_at_position);
        filesize_t next_offset = 0;
        read_filesize(value_at_position, next_offset);

        std::vector<uint8_t> key_at_position(current_node._key_size);

        current_node.get_key_at_n(get_value_pos, key_at_position);

        std::vector<uint8_t> child_node_key(current_node._key_size);
        filesize_t child_node_offset = 0;

        insert_recursive(next_offset, key, data, node_is_split, new_node_key, new_node_offset, child_node_key, child_node_offset);

        if (node_is_split)
        {
            insert_required = true;
            insert_value.resize(sizeof(filesize_t));
            insert_value_span = { insert_value };
            insert_key_span = { new_node_key };
            write_filesize(insert_value_span, new_node_offset);
        }

        if (compare_span(child_node_key, key_at_position) != 0)
        {
            // a key update is necessary
            auto key_start = current_node._data.begin() + (get_value_pos * current_node.get_entry_size());
            std::copy(child_node_key.begin(), child_node_key.end(), key_start);
            updated_occurred = true;
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



            // Adjust the current node data
            current_node._data.resize(mid_offset);

            // Insert the new key and data into the correct node
            if (insert_pos > mid_index)
            {
                insert_pos -= mid_index;
                new_node._data.insert(new_node._data.begin() + insert_pos * new_node.get_entry_size(), insert_key_span.begin(), insert_key_span.end());
                new_node._data.insert(new_node._data.begin() + insert_pos * new_node.get_entry_size() + new_node._key_size, insert_value_span.begin(), insert_value_span.end());
            }
            else
            {
                current_node._data.insert(current_node._data.begin() + insert_pos * current_node.get_entry_size(), insert_key_span.begin(), insert_key_span.end());
                current_node._data.insert(current_node._data.begin() + insert_pos * current_node.get_entry_size() + current_node._key_size, insert_value_span.begin(), insert_value_span.end());
            }

            // Set the new node key
            new_node_key.resize(new_node._key_size);
            new_node.get_key_at_n(0, new_node_key);

            // Write both nodes back
            new_node_offset = _file.get_file_size();
            new_node.write_node(new_node_offset);
            current_node.write_node(node_offset);

            node_is_split = true;
        }
        else
        {
            // Insert the key and data into the current node
            int offset = insert_pos * current_node.get_entry_size();
            current_node._data.insert(current_node._data.begin() + offset, key.begin(), key.end());
            current_node._data.insert(current_node._data.begin() + offset + current_node._key_size, insert_value_span.begin(), insert_value_span.end());
            current_node.write_node(node_offset);
        }
    }
    else if (updated_occurred)
    {
        // if we updated the key, we need to write the node back
        current_node.write_node(node_offset);
    }

    // this may tell the caller that it needs to update it's key for the current node
    current_node_key.resize(current_node._key_size);
    std::span<uint8_t> current_node_span{ current_node_key };

    current_node.get_key_at_n(0, current_node_span);
}


void logging_btree::read_value_at_key(filesize_t node_offset, const std::span<uint8_t>& key, bool& found, std::vector<uint8_t>& data)
{
    variable_btree_node current_node(*this);
    current_node.read_node(node_offset);

    found = false;
    int position = 0;
    int prev_position = -1;
    for (position = 0; position < current_node.get_value_count(); position++)
    {
        std::vector<uint8_t> key_at_n(current_node._key_size);
        current_node.get_key_at_n(position, key_at_n);
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
            std::vector<uint8_t> value_at_n(current_node._value_size);
            current_node.get_value_at_n(position, value_at_n);
            data.assign(value_at_n.begin(), value_at_n.end());
        }
    }
    else if (prev_position == -1)
    {
        found = false;
    }
    else
    {
        std::vector<uint8_t> value_at_n(current_node._value_size);
        current_node.get_value_at_n(prev_position, value_at_n);
        filesize_t next_offset = 0;
        read_filesize(value_at_n, next_offset);
        return read_value_at_key(next_offset, key, found, data);
    }
}