#pragma once

#include <vector>
#include <span>

#include "../include/core.hpp"
#include "../include/binary_iterator.hpp"
#include "../include/file_cache.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/span_iterator.hpp"

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

    virtual ~btree_node() = default;

    bool is_leaf() const;
    uint64_t get_transaction_id();
    void set_transaction_id(uint64_t transaction_id);
    uint16_t get_key_size();
    void set_key_size(uint16_t key_size);
    uint16_t get_entry_count();
    void set_entry_count(uint16_t value_count);
    uint32_t get_value_size();
    void set_value_size(uint32_t value_size);
    filesize_t calculate_buffer_size();
    filesize_t calculate_entry_count_from_buffer_size();
    std::span<uint8_t> get_key_at(int n);
    std::span<uint8_t> get_value_at(int n);
    bool should_split();


    metadata get_metadata();

    find_result find_key(const metadata& md, std::span<uint8_t> key);

    void insert_entry(const metadata& md, const find_result& fr, std::span<uint8_t> entry);
    void update_entry(const metadata& md, const find_result& fr, std::span<uint8_t> entry);
    void remove_key(const metadata& md, const find_result& fr);


    bool remove_key(std::span<uint8_t> key);
    uint32_t upsert_entry(std::span<uint8_t> entry, bool allow_replace = true);

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
