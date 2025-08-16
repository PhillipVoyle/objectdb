#pragma once

#include <vector>
#include <span>

#include "../include/core.hpp"
#include "../include/binary_iterator.hpp"
#include "../include/file_cache.hpp"
#include "../include/far_offset_ptr.hpp"
#include "../include/span_iterator.hpp"
#include "../include/btree_row_traits.hpp"

class btree;

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

    struct metadata
    {
        size_t header_size;
        size_t key_size;
        size_t value_size;
        size_t entry_count;
    };

    metadata get_metadata();
    uint16_t get_capacity(const metadata& md);

    btree& btree_;

    void internal_insert_entry(int position, std::span<uint8_t> entry);
    void internal_update_entry(int position, std::span<uint8_t> entry);

    int compare_keys(const std::span<uint8_t> k1, const std::span<uint8_t> k2);

public:

    btree_node(btree& bt) : btree_(bt)
    {
    }

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
    std::vector<uint8_t> get_key_at(int n);
    std::vector<uint8_t> get_value_at(int n);
    std::span<uint8_t> get_entry(int n);

    far_offset_ptr get_branch_value_at(int n);

    uint16_t get_capacity();

    bool should_split();
    bool should_merge();

    void split(btree_node& other);
    void merge(btree_node& other_node);
    bool is_full();

    void init_leaf();
    void init_root();

    find_result find_key(std::span<uint8_t> key);

    void insert_leaf_entry(int position, std::span<uint8_t> entry);
    void insert_branch_entry(int position, std::span<uint8_t> key, far_offset_ptr offset);

    void update_branch_entry(int position, std::span<uint8_t> key, far_offset_ptr offset);
    void update_leaf_entry(int position, std::span<uint8_t> entry);

    void remove_key(int position);

    bool remove_key(std::span<uint8_t> key);

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
