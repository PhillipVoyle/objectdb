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
    * uint16_t: flags (2 bytes) (including first bit)
    * uint16_t: value_count (2 byte)
    * uint16_t: key size (2 bytes)
    * uint16_t: value_size (2 bytes)
    * 
    * array of data
    * each is a key-value pair
    */

    // note the keys and values are not large enough to hold sizes of large data, but in any realistic large data case
    // we'll want to simply add references to the real data location here and for that, a far ptr is probably fine

    // in the case of non leaf nodes, the values will reliably be far pointers so will be 128 bit values i.e a file id and offset
    // that's hopefully plenty of space to hold realistic data sets for the near future

    std::vector<uint8_t> data;

    static const uint8_t is_leaf_bit_mask = 0x1;

    static const size_t transaction_id_offset = 0;
    static const size_t transaction_id_size = 8; // big enough?

    static const size_t flags_offset = transaction_id_offset + transaction_id_size;
    static const size_t flags_size = 2;
    static const size_t entry_count_offset = flags_offset + flags_size;
    static const size_t entry_count_size = 2;
    static const size_t key_size_offset = entry_count_offset + entry_count_size;
    static const size_t key_size_size = 2;
    static const size_t value_size_offset = key_size_offset + key_size_size;
    static const size_t value_size_size = 2;
    static const size_t data_offset = value_size_offset + value_size_size;

    static constexpr size_t get_header_size()
    {
        return data_offset;
    }

public:
    virtual ~btree_node() = default;

    virtual bool is_leaf() const
    {
        return (data[flags_offset] & is_leaf_bit_mask) != 0;
    }

    virtual uint64_t get_transaction_id()
    {
        std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
        span_iterator it(transaction_id_span);
        return read_uint64(it);
    }

    virtual void set_transaction_id(uint64_t transaction_id)
    {
        std::span<uint8_t> transaction_id_span(data.begin() + transaction_id_offset, transaction_id_size);
        span_iterator it(transaction_id_span);
        write_uint64(it, transaction_id);
    }

    virtual uint16_t get_key_size()
    {
        std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
        span_iterator key_size_iter(key_size_span);
        return read_uint16(key_size_iter);
    }

    virtual void set_key_size(uint16_t key_size)
    {
        std::span<uint8_t> key_size_span(data.begin() + key_size_offset, key_size_size);
        span_iterator key_size_iter(key_size_span);
        write_uint16(key_size_iter, key_size);
    }

    virtual uint16_t get_entry_count()
    {
        std::span<uint8_t> value_count_span(data.begin() + entry_count_offset, entry_count_size);
        span_iterator value_count_iter(value_count_span);
        return read_uint16(value_count_iter);
    }

    virtual void set_entry_count(uint16_t value_count)
    {
        std::span<uint8_t> value_count_span(data.begin() + entry_count_offset, entry_count_size);
        span_iterator value_count_iter(value_count_span);
        write_uint16(value_count_iter, value_count);
    }

    virtual uint32_t get_value_size()
    {
        std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
        span_iterator value_size_iter(value_size_span);
        return read_uint16(value_size_iter);
    }

    virtual void set_value_size(uint32_t value_size)
    {
        std::span<uint8_t> value_size_span(data.begin() + value_size_offset, value_size_size);
        span_iterator value_size_iter(value_size_span);
        write_uint16(value_size_iter, value_size);
    }

    virtual filesize_t calculate_buffer_size()
    {
        filesize_t key_size = get_key_size();
        filesize_t value_count = get_entry_count();
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

    struct metadata
    {
        size_t header_size;
        size_t key_size;
        size_t value_size;
        size_t entry_count;
    };

    struct find_result
    {
        uint32_t position;
        bool found;
    };

    metadata get_metadata()
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

    find_result find_key(const metadata& md, std::span<uint8_t> key)
    {
        bool found = false;
        int n;
        for (;;)
        {
            if (n >=  md.entry_count)
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

    void upsert_entry(const metadata& md, const find_result& fr, std::span<uint8_t> entry)
    {
        size_t pair_size = static_cast<size_t>(md.key_size + md.value_size);
        size_t offset = md.header_size + fr.position * pair_size;

        bool replace = fr.found;
        if (!replace)
        {
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

        }

        std::span<uint8_t> destination(data.begin() + offset, pair_size);
        std::copy(entry.begin(), entry.end(), destination);

        set_entry_count(calculate_entry_count_from_buffer_size());
    }

    uint32_t upsert_entry(std::span<uint8_t> entry, bool allow_replace = true)
    {
        auto metadata = get_metadata();
        size_t pair_size = static_cast<size_t>(metadata.key_size + metadata.value_size);
        if (entry.size() != pair_size)
        {
            throw object_db_exception("expected an entry of correct size");
        }
        std::span<uint8_t> new_key(entry.begin(), metadata.key_size);
        auto fr = find_key(metadata, new_key);

        bool replace = fr.found;
        if (replace)
        {
            if (!allow_replace)
            {
                throw object_db_exception("disallowed replace");
            }
        }
        upsert_entry(metadata, fr, entry);
        return fr.position;
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
