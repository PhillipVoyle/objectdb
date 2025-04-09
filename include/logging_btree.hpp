#pragma once

#include "../include/random_access_file.hpp"

#include <vector>
#include <cassert>


class logging_btree;

class variable_btree_node
{
    logging_btree& _btree;
    random_access_file& file();
    filesize_t _offset = 0;
public:
    bool _is_leaf = 0;
    uint32_t _key_size = 0;
    uint32_t _value_size = 0;

    std::vector<uint8_t> _data;

    explicit variable_btree_node(logging_btree& btree);

    filesize_t get_offset() const;

    bool read_node(filesize_t offset);

    int get_entry_size() const;

    int get_value_count() const;

    bool write_node(filesize_t offset);

    filesize_t get_node_size() const;

    bool get_key_at_n(int n, std::span<const uint8_t>& key) const;

    bool get_value_at_n(int n, std::span<const uint8_t>& value) const;
};

class key_offset
{
public:
    filesize_t node_offset;
    int value_offset;
    int comparison;
};

class value_location
{
public:
    std::vector<key_offset> best_value_position;
};

int compare_span(std::span<const uint8_t> a, std::span<const uint8_t> b);

class logging_btree
{
private:
    friend class variable_btree_node;
    random_access_file& _file;
    int key_size = 0;
    int value_size = 0;

    bool internal_find_key(variable_btree_node& node, const std::span<uint8_t>& key, value_location& location);

public:
    logging_btree(random_access_file& file);
    bool create_empty_root_node(filesize_t offset, filesize_t& root_offset);

    bool find_key(int root_offset, const std::span<uint8_t>& key, value_location& location);
};