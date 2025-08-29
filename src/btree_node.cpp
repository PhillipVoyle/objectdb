#pragma once

#include "../include/btree_node.hpp"
#include "../include/btree.hpp"

// Returns true if this node is a leaf node (no children)
bool btree_node::is_leaf() const
{
    return (data[flags_offset] & is_leaf_bit_mask) != 0;
}

// Finds the position of a key in the node. Returns whether found and the position to insert/search.
btree_node::find_result btree_node::find_key(std::span<uint8_t> key)
{
    auto md = get_metadata();
    bool found = false;
    int n = 0;
    for (;;)
    {
        if (n >= md.entry_count)
        {
            break;
        }

        auto key_at_n = get_key_at(n);
        int cmp = compare_keys(key, key_at_n);
        if (cmp < 0)
        {
            break;
        }
        else if (cmp == 0)
        {
            found = true;
            break;
        }
        n++;
    }
    find_result result;
    result.position = n;
    result.found = found;
    return result;
}

// Compares two keys using the btree's key comparison logic
int btree_node::compare_keys(std::span<uint8_t> key_a, std::span<uint8_t> key_b)
{
    return btree_.compare_keys(key_a, key_b);
}

// Removes a key from the node if it exists. Returns true if the key was found and removed.
bool btree_node::remove_key(std::span<uint8_t> key)
{
    auto md = get_metadata();
    auto fr = find_key(key);
    if (fr.found)
    {
        remove_key(fr.position);
    }
    return fr.found;
}

// Reads the transaction ID from the node header
uint64_t btree_node::get_transaction_id()
{
    std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
    span_iterator it(transaction_id_span);
    return read_uint64(it);
}

// Sets the transaction ID in the node header
void btree_node::set_transaction_id(uint64_t transaction_id)
{
    std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
    span_iterator it(transaction_id_span);
    write_uint64(it, transaction_id);
}

// Gets the key size from the node header
uint16_t btree_node::get_key_size()
{
    std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
    span_iterator key_size_iter(key_size_span);
    return read_uint16(key_size_iter);
}

// Sets the key size in the node header
void btree_node::set_key_size(uint16_t key_size)
{
    std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
    span_iterator key_size_iter(key_size_span);
    write_uint16(key_size_iter, key_size);
}

// Gets the number of entries (key-value pairs) in the node
uint16_t btree_node::get_entry_count()
{
    std::span<uint8_t> value_count_span(data.begin() + entry_count_offset, entry_count_size);
    span_iterator value_count_iter(value_count_span);
    return read_uint16(value_count_iter);
}

// Sets the number of entries in the node and resizes the data buffer accordingly
void btree_node::set_entry_count(uint16_t value_count)
{
    auto md = get_metadata();
    std::span<uint8_t> value_count_span(data.begin() + entry_count_offset, entry_count_size);
    span_iterator value_count_iter(value_count_span);
    write_uint16(value_count_iter, value_count);

    data.resize(md.header_size + (value_count * (md.key_size + md.value_size)));
}

// Gets the value size from the node header
uint32_t btree_node::get_value_size()
{
    std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
    span_iterator value_size_iter(value_size_span);
    return read_uint16(value_size_iter);
}

// Sets the value size in the node header
void btree_node::set_value_size(uint32_t value_size)
{
    std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
    span_iterator value_size_iter(value_size_span);
    write_uint16(value_size_iter, value_size);
}

// Calculates the total buffer size needed for the node (header + all entries)
filesize_t btree_node::calculate_buffer_size()
{
    auto md = get_metadata();
    return md.entry_count * (md.key_size + md.value_size) + md.header_size;
}

// Calculates the number of entries based on the current buffer size
filesize_t btree_node::calculate_entry_count_from_buffer_size()
{
    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    if (data.size() < get_header_size())
        throw object_db_exception("could not calculate buffer size");

    auto data_size = data.size() - get_header_size();

    return data_size / (key_size + value_size);
}

// Retrieves the key at the given entry index
std::vector<uint8_t> btree_node::get_key_at(int n)
{
    auto entry_span = get_entry(n);

    if (is_leaf())
    {
        // key is in natural position
        return btree_.get_row_traits()->get_key_traits()->get_data(entry_span, &btree_.get_heap());
    }
    else
    {
        // In branch nodes, keys are stored raw
        filesize_t key_size = get_key_size();
        std::span<uint8_t> data_span{ entry_span.begin(), static_cast<size_t>(key_size) };
        return std::vector<uint8_t>(data_span.begin(), data_span.end());
    }
}

// Retrieves the value at the given entry index
std::vector<uint8_t> btree_node::get_value_at(int n)
{
    auto entry_span = get_entry(n);
    if (is_leaf())
    {
        // In leaf nodes, values are stored using row traits
        return btree_.get_row_traits()->get_value_traits()->get_data(entry_span, &btree_.get_heap());
    }

    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    auto span = std::span<uint8_t>(entry_span.begin() + key_size, value_size);
    return std::vector<uint8_t>(span.begin(), span.end());
}

// Gets the far_offset_ptr value at the given entry index (for branch nodes)
far_offset_ptr btree_node::get_branch_value_at(int n)
{
    if (is_leaf())
    {
        throw object_db_exception("cannot get branch value if node is a leaf");
    }
    auto entry_span = get_entry(n);
    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    auto span = std::span<uint8_t>(entry_span.begin() + key_size, value_size);
    span_iterator it{ span };
    far_offset_ptr result;
    result.read(it);
    return result;
}

// Returns a span representing the nth entry (key-value pair) in the node
std::span<uint8_t> btree_node::get_entry(int n)
{
    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    // Calculate the offset to the nth key-value pair
    size_t header_size = get_header_size();
    size_t pair_size = static_cast<size_t>(key_size + value_size);

    size_t offset = header_size + n * pair_size;

    if (offset + key_size > data.size())
        throw std::out_of_range("Key index out of range");

    return std::span<uint8_t>(data.data() + offset, static_cast<size_t>(key_size + value_size));
}

// Returns true if the node should be split (i.e., it exceeds the block size)
bool btree_node::should_split()
{
    return data.size() > block_size;
}

// Calculates the maximum number of entries that can fit in a node, given metadata
uint16_t btree_node::get_capacity(const metadata& md)
{
    return (block_size - md.header_size) / (md.key_size + md.value_size);
}

// Returns the capacity for this node, based on its metadata
uint16_t btree_node::get_capacity()
{
    auto md = get_metadata();
    return get_capacity(md);
}

// Returns true if the node should be merged (i.e., it is less than half full)
bool btree_node::should_merge()
{
    auto md = get_metadata();
    return get_entry_count() < (get_capacity(md) / 2);
}

// Merges another node's entries into this node, clearing the other node
void btree_node::merge(btree_node& other_node)
{
    auto other_entry_count = other_node.get_entry_count();

    for (uint32_t a = 0; a < other_entry_count; a++)
    {
        auto entry_count = get_entry_count();
        auto entry_span = other_node.get_entry(a);
        internal_insert_entry(entry_count, entry_span);
    }
    other_node.set_entry_count(0);
}

// Returns true if the node is full (cannot fit another entry)
bool btree_node::is_full()
{
    auto md = get_metadata();
    filesize_t key_size = md.key_size;
    filesize_t value_size = md.value_size;
    if ((data.size() + key_size + value_size) >= block_size)
    {
        return true;
    }
    return false;
}

// Returns a metadata struct describing the node's layout and entry count
btree_node::metadata btree_node::get_metadata()
{
    auto count = get_entry_count();

    size_t header_size = get_header_size();
    size_t key_size = get_key_size();
    size_t value_size = get_value_size();

    metadata result;
    result.header_size = header_size;
    result.key_size = key_size;
    result.value_size = value_size;
    result.entry_count = count;

    return result;
}

// Inserts an entry at the given position, shifting later entries as needed
void btree_node::internal_insert_entry(int position, std::span<uint8_t> entry)
{
    auto md = get_metadata();
    size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
    size_t offset = md.header_size + position * pair_size;
    set_entry_count(md.entry_count + 1);
    // Move existing data at and after the insert position back by one pair_size
    if (position < md.entry_count)
    {
        std::memmove(
            data.data() + offset + pair_size,
            data.data() + offset,
            (md.entry_count - position) * pair_size
        );
    }
    std::span<uint8_t> destination(data.begin() + offset, pair_size);
    std::copy(entry.begin(), entry.end(), destination.begin());
}

// Inserts a branch (internal node) entry at the given position
void btree_node::insert_branch_entry(int position, std::span<uint8_t> key, far_offset_ptr offset)
{
    std::vector<uint8_t> new_entry(key.size() + far_offset_ptr::get_size());
    std::copy(key.begin(), key.end(), new_entry.begin());

    span_iterator value_it({ new_entry.begin() + key.size(), new_entry.size() - key.size() });
    offset.write(value_it);
    internal_insert_entry(position, new_entry);
}

// Inserts a leaf entry at the given position
void btree_node::insert_leaf_entry(int position, std::span<uint8_t> entry)
{
    internal_insert_entry(position, entry);
}

// Updates an entry at the given position with new data
void btree_node::internal_update_entry(int position, std::span<uint8_t> entry)
{
    auto md = get_metadata();
    size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
    size_t offset = md.header_size + position * pair_size;

    std::span<uint8_t> destination(data.begin() + offset, pair_size);
    std::copy(entry.begin(), entry.end(), destination.begin());
}

// Updates a branch entry at the given position
void btree_node::update_branch_entry(int position, std::span<uint8_t> key, far_offset_ptr reference)
{
    std::vector<uint8_t> new_node_entry(get_key_size() + far_offset_ptr::get_size());
    std::copy(key.begin(), key.end(), new_node_entry.begin());
    std::span<uint8_t> new_value_span{
        new_node_entry.begin() + get_key_size(),
        new_node_entry.end()
    };

    auto new_entry_span_it = span_iterator{ new_value_span };
    reference.write(new_entry_span_it);

    internal_update_entry(position, new_node_entry);
}

// Updates a leaf entry at the given position
void btree_node::update_leaf_entry(int position, std::span<uint8_t> entry)
{
    internal_update_entry(position, entry);
}

// Removes the entry at the given position, shifting later entries forward
void btree_node::remove_key(int position)
{
    auto md = get_metadata();
    size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
    size_t offset = md.header_size + position * pair_size;
    if (position < (md.entry_count - 1))
    {
        std::memmove(
            data.data() + offset,
            data.data() + offset + pair_size,
            (md.entry_count - position - 1) * pair_size
        );
    }

    set_entry_count(md.entry_count - 1);
}

// Initializes this node as a leaf node
void btree_node::init_leaf()
{
    data.resize(data_offset + get_header_size());
    data[flags_offset] |= is_leaf_bit_mask; // Set the leaf bit
    set_key_size(0);
    set_value_size(0);
    set_entry_count(0);
    set_transaction_id(0);
}

// Initializes this node as a root (non-leaf) node
void btree_node::init_root()
{
    data.resize(data_offset + get_header_size());
    set_key_size(0);
    set_value_size(0);
    set_entry_count(0);
    set_transaction_id(0);
}

// Splits this node into two, moving half the entries to overflow_node
void btree_node::split(btree_node& overflow_node)
{
    // determine the median
    auto md = get_metadata();
    auto count = md.entry_count;
    auto half_way = (uint32_t) (count / 2);
    overflow_node.data.resize(overflow_node.get_header_size());
    overflow_node.data[flags_offset] = data[flags_offset];
    overflow_node.set_key_size(md.key_size);
    overflow_node.set_value_size(md.value_size);
    overflow_node.set_entry_count((int16_t)(count - half_way));

    uint32_t position = 0;
    for (uint32_t i = half_way; i < count; i++)
    {
        auto entry_span = get_entry(i);
        overflow_node.internal_update_entry(position, entry_span);

        position++;
    }

    set_entry_count((uint16_t)(half_way));
}


