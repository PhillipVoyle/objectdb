#pragma once

#include <memory>

#include "../include/core.hpp"

class btree_data_traits
{
public:
    virtual ~btree_data_traits() = default;
    virtual int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) = 0;
    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span) = 0;
    virtual uint32_t get_size() = 0;
};

class btree_row_traits
{
public:
    virtual ~btree_row_traits() = default;
    virtual std::shared_ptr<btree_data_traits> get_key_traits() = 0;
    virtual std::shared_ptr<btree_data_traits> get_value_traits() = 0;
    virtual std::shared_ptr<btree_data_traits> get_entry_traits() = 0;
};
