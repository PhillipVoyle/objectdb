#pragma once

#include <vector>
#include <span>

#include "../include/core.hpp"
#include "../include/binary_iterator.hpp"
#include "../include/file_cache.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/span_iterator.hpp"

int compare_span(const std::span<uint8_t>& a, const std::span<uint8_t>& b);

class btree_node
{
    /*
    * Memory layout of a btree node:
    * uint64_t: transaction_id (8 bytes)
    * uint8_t: is_leaf_node (1 byte)
    * uint8_t: value_count (1 byte)
    * uint16_t: key size (2 bytes)
    * uint32_t: value size (4 bytes) (this will be a value of 16 to accomodate far_offset_ptr for branch nodes)
    * array of data
    * each is a key-value pair
    */

    std::vector<uint8_t> data;

    static const size_t transaction_id_offset = 0;
    static const size_t transaction_id_size = 8; // big enough?

    static const size_t is_leaf_node_offset = transaction_id_offset + transaction_id_size;
    static const size_t is_leaf_node_size = 1;
    static const size_t value_count_offset = is_leaf_node_offset + is_leaf_node_size;
    static const size_t value_count_size = 1;
    static const size_t key_size_offset = value_count_offset + value_count_size;
    static const size_t key_size_size = 2;
    static const size_t value_size_offset = key_size_offset + key_size_size;
    static const size_t value_size_size = 4;
    static const size_t data_offset = value_size_offset + value_size_size;

    static constexpr size_t get_header_size()
    {
        return data_offset;
    }

public:
    virtual ~btree_node() = default;

    virtual bool is_leaf() const
    {
        return data[is_leaf_node_offset] != 0;
    }

    virtual uint64_t get_transaction_id()
    {
        std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
        span_iterator it(transaction_id_span);
        return read_uint64(it);
    }

    virtual uint16_t get_key_size()
    {
        std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
        span_iterator key_size_iter(key_size_span);
        return read_uint16(key_size_iter);
    }

    virtual uint8_t get_value_count()
    {
        return data[1];
    }

    virtual uint32_t get_value_size()
    {
        std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
        span_iterator value_size_iter(value_size_span);
        return read_uint16(value_size_iter);
    }

    virtual filesize_t calculate_buffer_size()
    {
        filesize_t key_size = get_key_size();
        filesize_t value_count = get_value_count();
        filesize_t value_size = get_value_size();

        return value_count * (key_size + value_size);
    }

    virtual uint32_t calculate_entry_count_from_buffer_size()
    {
        filesize_t key_size = get_key_size();
        filesize_t value_size = get_value_size();

        if (data.size() < get_header_size())
            throw object_db_exception("could not calculate buffer size");

        auto data_size = data.size() - get_header_size();

        return data_size / (key_size + value_size);
    }

    std::span<uint8_t> get_key_at(int n)
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

    std::span<uint8_t> get_value_at(int n)
    {
        filesize_t key_size = get_key_size();
        filesize_t value_size = get_value_size();

        // Calculate the offset to the nth key-value pair
        // Layout: [0] is_leaf (1 byte), [1-3] key_size (3 bytes), [4-7] value_size (4 bytes)
        size_t header_size = 1 + 3 + 4;
        size_t pair_size = static_cast<size_t>(key_size + value_size);

        size_t offset = header_size + n * pair_size + key_size;

        if (offset + value_size > data.size())
            throw std::out_of_range("Value index out of range");

        return std::span<uint8_t>(data.data() + offset, static_cast<size_t>(key_size));
    }

    bool should_split()
    {
        return calculate_entry_count_from_buffer_size() > 255;
    }

    uint32_t upsert_entry(std::span<uint8_t> entry)
    {
        std::span<uint8_t> new_key(entry.begin(), get_key_size());
        int n = 0;
        bool replace = false;
        auto count = calculate_entry_count_from_buffer_size();

        size_t header_size = get_header_size();
        size_t key_size = get_key_size();
        size_t value_size = get_value_size();

        size_t pair_size = static_cast<size_t>(key_size + value_size);
        if (entry.size() != pair_size)
        {
            throw object_db_exception("expected an entry of correct size");
        }

        for (;;)
        {
            if (n >= count)
            {
                break;
            }

            std::span<uint8_t> key = get_key_at(n);
            int cmp = compare_span(key, new_key);
            if (cmp < 0)
            {
                break;
            }
            else if (cmp == 0)
            {
                replace = true;
            }
        }

        size_t offset = header_size + n * pair_size;

        if (!replace)
        {
            data.resize(data_offset + pair_size * (count + 1));

            // Move existing data at and after the insert position back by one pair_size
            if (n < count)
            {
                std::memmove(
                    data.data() + offset + pair_size,
                    data.data() + offset,
                    (count - n) * pair_size
                );
            }
        }

        std::span<uint8_t> destination(data.begin() + offset, pair_size);
        std::copy(entry.begin(), entry.end(), destination);
    }

    template<Binary_iterator It>
    void write(It& it)
    {
        write_span(it, data);
    }

    template<Binary_iterator It>
    void read(It& it)
    {
        // start by reading the header
        auto header_size = get_header_size();
        data.resize(header_size);
        read_span(it, data);

        auto size = calculate_buffer_size();
        data.resize(size);

        // fill remainder of buffer
        read_span(it, { data.begin() + header_size, data.begin() + size });
    }
};
