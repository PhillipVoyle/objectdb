#pragma once

#include "../include/btree_row_traits.hpp"
#include "../include/span_iterator.hpp"
#include "../include/binary_iterator.hpp"
#include "../include/core.hpp"
#include <format>

class table_data_traits;

class field_data_traits : public btree_data_traits
{
    uint32_t offset_;
    uint32_t size_;
public:
    field_data_traits(uint32_t offset, uint32_t size);

    uint32_t get_offset();

    virtual uint32_t get_size() override;
    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span);
};

class int32_field : public field_data_traits
{
public:
    int32_field(uint32_t offset);
    int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) override;
};

class uint32_field : public field_data_traits
{
public:
    uint32_field(uint32_t offset);
    int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) override;
};

class span_field : public field_data_traits
{
public:
    span_field(uint32_t offset, uint32_t size);
    int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) override;
};

class entry_data_traits : public btree_data_traits
{
public:
    std::vector<std::shared_ptr<field_data_traits>> fields_;
    entry_data_traits(const std::vector<std::shared_ptr<field_data_traits>>& fields);
    virtual int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2);
    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span) override;
    virtual uint32_t get_size() override;
};

class reference_data_traits : public btree_data_traits
{
    std::shared_ptr<entry_data_traits> entry_traits_;
    std::vector<int> field_references_;
public:
    reference_data_traits(std::shared_ptr<entry_data_traits> entry_traits, const std::vector<int> field_references);
    virtual int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) final;
    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span) final;

    virtual uint32_t get_size() final;
};

class table_row_traits : public btree_row_traits
{
    std::shared_ptr<entry_data_traits> entry_traits_;
    std::shared_ptr<reference_data_traits> key_traits_;
    std::shared_ptr<reference_data_traits> value_traits_;
public:
    table_row_traits(std::shared_ptr<entry_data_traits> traits, std::vector<int> key_references);

    // Inherited via btree_row_traits
    std::shared_ptr<btree_data_traits> get_key_traits() override;
    std::shared_ptr<btree_data_traits> get_value_traits() override;
    std::shared_ptr<btree_data_traits> get_entry_traits() override;

};

class table_row_traits_builder
{
    std::vector<std::shared_ptr<field_data_traits>> field_traits_;
    std::vector<int> field_references_;

    uint32_t get_current_offset();
public:
    int add_span_field(int size);
    int add_uint32_field();
    int add_int32_field();
    void add_key_reference(int position);

    std::shared_ptr<table_row_traits> create_table_row_traits();
};
