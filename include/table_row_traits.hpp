#pragma once

#include "../include/btree_row_traits.hpp"
#include "../include/span_iterator.hpp"
#include "../include/binary_iterator.hpp"
#include <format>

class table_data_traits;

class field_data_traits : public btree_data_traits
{
    uint32_t offset_;
    uint32_t size_;
public:
    field_data_traits(uint32_t offset, uint32_t size) :
        offset_(offset),
        size_(size)
    {
    }

    uint32_t get_offset()
    {
        return offset_;
    }

    uint32_t get_size()
    {
        return size_;
    }

    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span)
    {
        return std::vector<uint8_t>(entry_span.begin() + offset_, entry_span.begin() + offset_ + size_);
    }
};

class int32_field : public field_data_traits
{
public:
    int32_field(uint32_t offset) : field_data_traits(offset, 4)
    {
    }

    int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) override
    {
        span_iterator it1{ p1 };
        span_iterator it2{ p2 };
        auto n1 = read_int32(it1);
        auto n2 = read_int32(it2);

        return n1 < n2;
    }
};

class uint32_field : public field_data_traits
{
public:
    uint32_field(uint32_t offset) : field_data_traits(offset, 4)
    {
    }

    int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) override
    {
        span_iterator it1{ p1 };
        span_iterator it2{ p2 };
        auto n1 = read_uint32(it1);
        auto n2 = read_uint32(it2);

        return n1 < n2;
    }
};

class span_field : public field_data_traits
{
public:
    span_field(uint32_t offset, uint32_t size) : field_data_traits(offset, size)
    {
    }

    int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2) override
    {
        return compare_span(p1, p2);
    }
};

class entry_data_traits : public btree_data_traits
{
public:
    std::vector<std::shared_ptr<field_data_traits>> fields_;
    entry_data_traits(const std::vector<std::shared_ptr<field_data_traits>>& fields) :
        fields_(fields)
    {
    }

    virtual int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
    {
        for (auto field : fields_)
        {
            // todo: do we need to use get_data? probably yes if we need to load long data from a different file or something.
            // Perhaps should be optional to avoid the allocation and copy
            std::span<uint8_t> s1 { p1.begin() + field->get_offset(), (size_t)field->get_size() };
            std::span<uint8_t> s2 { p2.begin() + field->get_offset(), (size_t)field->get_size() };

            auto result = field->compare(s1, s2);
            if (result != 0)
            {
                return result;
            }
        }
        return 0;
    }
    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span)
    {
        std::vector<uint8_t> result;
        size_t position = 0;
        for (auto field : fields_)
        {
            auto size = (size_t)field->get_size();
            std::copy_n(entry_span.begin() + field->get_offset(), size, std::back_inserter(result));
        }
        return result;
    }
};

class reference_data_traits : public btree_data_traits
{
    std::shared_ptr<entry_data_traits> entry_traits_;
    std::vector<int> field_references_;
public:
    reference_data_traits(std::shared_ptr<entry_data_traits> entry_traits, const std::vector<int> field_references)
    {
    }

    virtual int compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
    {
        for (auto field_reference : field_references_)
        {
            auto field = entry_traits_->fields_[field_reference];
            // todo: do we need to use get_data? probably yes if we need to load long data from a different file or something.
            // Perhaps should be optional to avoid the allocation and copy
            std::span<uint8_t> s1{ p1.begin() + field->get_offset(), (size_t)field->get_size() };
            std::span<uint8_t> s2{ p2.begin() + field->get_offset(), (size_t)field->get_size() };

            auto result = field->compare(s1, s2);
            if (result != 0)
            {
                return result;
            }
        }
        return 0;
    }
    virtual std::vector<uint8_t> get_data(const std::span<uint8_t>& entry_span)
    {
        std::vector<uint8_t> result;
        size_t position = 0;
        for (auto field_reference : field_references_)
        {
            auto field = entry_traits_->fields_[field_reference];

            auto size = (size_t)field->get_size();
            std::copy_n(entry_span.begin() + field->get_offset(), size, std::back_inserter(result));
        }
        return result;
    }
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
public:
    int add_span_field(int size);
    int add_uint32_field();
    int add_int32_field();
    void add_key_reference(int position);

    std::shared_ptr<table_row_traits> create_table_row_traits();
};
