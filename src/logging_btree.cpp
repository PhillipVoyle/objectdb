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

bool logging_btree::create_empty_root_node(filesize_t offset, filesize_t& root_offset)
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

    root_offset = offset + node.get_node_size();
    return true;
}

bool logging_btree::find_key(int root_offset, const std::span<uint8_t>& key, value_location& location)
{
    variable_btree_node node(*this);
    node.read_node(root_offset);

    return internal_find_key(node, key, location);
}
