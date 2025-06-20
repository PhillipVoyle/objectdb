#pragma once

#include "../include/btree_node.hpp"

bool btree_node::is_leaf() const
{
    return (data[flags_offset] & is_leaf_bit_mask) != 0;
}

uint64_t btree_node::get_transaction_id()
{
    std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
    span_iterator it(transaction_id_span);
    return read_uint64(it);
}

void btree_node::set_transaction_id(uint64_t transaction_id)
{
    std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
    span_iterator it(transaction_id_span);
    write_uint64(it, transaction_id);
}

uint16_t btree_node::get_key_size()
{
    std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
    span_iterator key_size_iter(key_size_span);
    return read_uint16(key_size_iter);
}

void btree_node::set_key_size(uint16_t key_size)
{
    std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
    span_iterator key_size_iter(key_size_span);
    write_uint16(key_size_iter, key_size);
}

uint16_t btree_node::get_entry_count()
{
    std::span<uint8_t> value_count_span(data.begin() + entry_count_offset, entry_count_size);
    span_iterator value_count_iter(value_count_span);
    return read_uint16(value_count_iter);
}

void btree_node::set_entry_count(uint16_t value_count)
{
    std::span<uint8_t> value_count_span(data.begin() + entry_count_offset, entry_count_size);
    span_iterator value_count_iter(value_count_span);
    write_uint16(value_count_iter, value_count);
}

uint32_t btree_node::get_value_size()
{
    std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
    span_iterator value_size_iter(value_size_span);
    return read_uint16(value_size_iter);
}

void btree_node::set_value_size(uint32_t value_size)
{
    std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
    span_iterator value_size_iter(value_size_span);
    write_uint16(value_size_iter, value_size);
}

filesize_t btree_node::calculate_buffer_size()
{
    filesize_t key_size = get_key_size();
    filesize_t value_count = get_entry_count();
    filesize_t value_size = get_value_size();

    return value_count * (key_size + value_size);
}

filesize_t btree_node::calculate_entry_count_from_buffer_size()
{
    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    if (data.size() < get_header_size())
        throw object_db_exception("could not calculate buffer size");

    auto data_size = data.size() - get_header_size();

    return data_size / (key_size + value_size);
}

std::span<uint8_t> btree_node::get_key_at(int n)
{
    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    // Calculate the offset to the nth key-value pair
    // Layout: [0] is_leaf (1 byte), [1-3] key_size (3 bytes), [4-7] value_size (4 bytes)
    size_t header_size = get_header_size();
    size_t pair_size = static_cast<size_t>(key_size + value_size);

    size_t offset = header_size + n * pair_size;

    if (offset + key_size > data.size())
        throw std::out_of_range("Key index out of range");

    return std::span<uint8_t>(data.data() + offset, static_cast<size_t>(key_size));
}

std::span<uint8_t> btree_node::get_value_at(int n)
{
    filesize_t key_size = get_key_size();
    filesize_t value_size = get_value_size();

    // Calculate the offset to the nth key-value pair
    // Layout: [0] is_leaf (1 byte), [1-3] key_size (3 bytes), [4-7] value_size (4 bytes)
    size_t header_size = 1 + 3 + 4;
    size_t pair_size = static_cast<size_t>(key_size + value_size);

    size_t offset = header_size + n * pair_size + key_size;

    if (offset + value_size > data.size())
    {
        throw std::out_of_range("Value index out of range");
    }

    return std::span<uint8_t>(data.data() + offset, static_cast<size_t>(key_size));
}

bool btree_node::should_split()
{
    return calculate_entry_count_from_buffer_size() > 255;
}

bool btree_node::is_full()
{
    return calculate_entry_count_from_buffer_size() >= 255;
}

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

btree_node::find_result btree_node::find_key(const btree_node::metadata& md, std::span<uint8_t> key)
{
    bool found = false;
    int n = 0;
    for (;;)
    {
        if (n >= md.entry_count)
        {
            break;
        }

        std::span<uint8_t> key = get_key_at(n);
        int cmp = compare_span(key, key);
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

void btree_node::insert_entry(const btree_node::metadata& md, const btree_node::find_result& fr, std::span<uint8_t> entry)
{
    size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
    size_t offset = md.header_size + fr.position * pair_size;
    data.resize(data_offset + pair_size * (md.entry_count + 1));
    // Move existing data at and after the insert position back by one pair_size
    if (fr.position < md.entry_count)
    {
        std::memmove(
            data.data() + offset + pair_size,
            data.data() + offset,
            (md.entry_count - fr.position) * pair_size
        );
    }
    std::span<uint8_t> destination(data.begin() + offset, pair_size);
    std::copy(entry.begin(), entry.end(), destination.begin());

    set_entry_count(calculate_entry_count_from_buffer_size());
}

void btree_node::update_entry(const btree_node::metadata& md, const btree_node::find_result& fr, std::span<uint8_t> entry)
{
    size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
    size_t offset = md.header_size + fr.position * pair_size;
    bool replace = fr.found;

    std::span<uint8_t> destination(data.begin() + offset, pair_size);
    std::copy(entry.begin(), entry.end(), destination.begin());

    set_entry_count(calculate_entry_count_from_buffer_size());
}

void btree_node::remove_key(const btree_node::metadata& md, const btree_node::find_result& fr)
{
    size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
    size_t offset = md.header_size + fr.position * pair_size;
    if (fr.position < (md.entry_count - 1))
    {
        std::memmove(
            data.data() + offset,
            data.data() + offset + pair_size,
            (md.entry_count - fr.position - 1) * pair_size
        );
    }

    data.resize(data_offset + pair_size * (md.entry_count - 1));
}

bool btree_node::remove_key(std::span<uint8_t> key)
{
    auto md = get_metadata();
    auto fr = find_key(md, key);
    if (fr.found)
    {
        remove_key(md, fr);
    }
    return fr.found;
}

uint32_t btree_node::upsert_entry(std::span<uint8_t> entry, bool allow_replace)
{
    auto metadata = get_metadata();
    size_t pair_size = static_cast<size_t>(metadata.key_size + metadata.value_size);
    if (entry.size() != pair_size)
    {
        throw object_db_exception("expected an entry of correct size");
    }
    std::span<uint8_t> new_key(entry.begin(), metadata.key_size);
    auto fr = find_key(metadata, new_key);

    if (fr.found)
    {
        if (!allow_replace)
        {
            throw object_db_exception("disallowed replace");
        }
        update_entry(metadata, fr, entry);
    }
    else
    {
        insert_entry(metadata, fr, entry);
    }
    return fr.position;
}

