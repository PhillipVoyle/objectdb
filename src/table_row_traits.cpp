#include "../include/table_row_traits.hpp"
#include <algorithm>
#include <numeric>

table_row_traits::table_row_traits(std::shared_ptr<entry_data_traits> traits, std::vector<int> key_references) : entry_traits_(traits)
{
    std::vector<int> data_references;
    std::vector<bool> used_references(entry_traits_->fields_.size(), false);
    for (int i = 0; i < key_references.size(); i++)
    {
        auto reference = key_references[i];
        if (reference < 0 || reference >= entry_traits_->fields_.size())
        {
            throw object_db_exception(std::format("invalid field reference: {}", reference));
        }
        used_references[reference] = true;
    }
    for (int i = 0; i < used_references.size(); i++)
    {
        if (!used_references[i])
        {
            data_references.push_back(i);
        }
    }
    key_traits_ = std::make_shared<reference_data_traits>(entry_traits_, key_references);
    value_traits_ = std::make_shared<reference_data_traits>(entry_traits_, data_references);
}

std::shared_ptr<btree_data_traits> table_row_traits::get_key_traits()
{
    return key_traits_;
}

std::shared_ptr<btree_data_traits> table_row_traits::get_value_traits()
{
    return value_traits_;
}

std::shared_ptr<btree_data_traits> table_row_traits::get_entry_traits()
{
    return entry_traits_;
}

field_data_traits::field_data_traits(uint32_t offset, uint32_t size) :
    offset_(offset),
    size_(size)
{
}

uint32_t field_data_traits::get_offset()
{
    return offset_;
}

uint32_t field_data_traits::get_size()
{
    return size_;
}

std::vector<uint8_t> field_data_traits::get_data(const std::span<uint8_t>& entry_span)
{
    return std::vector<uint8_t>(entry_span.begin() + offset_, entry_span.begin() + offset_ + size_);
}


int32_field::int32_field(uint32_t offset) : field_data_traits(offset, 4)
{
}

int int32_field::compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
{
    span_iterator it1{ p1 };
    span_iterator it2{ p2 };
    auto n1 = read_int32(it1);
    auto n2 = read_int32(it2);

    return n1 < n2;
}

uint32_field::uint32_field(uint32_t offset) : field_data_traits(offset, 4)
{
}

int uint32_field::compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
{
    span_iterator it1{ p1 };
    span_iterator it2{ p2 };
    auto n1 = read_uint32(it1);
    auto n2 = read_uint32(it2);

    return n1 < n2;
}

span_field::span_field(uint32_t offset, uint32_t size) : field_data_traits(offset, size)
{
}

int span_field::compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
{
    return compare_span(p1, p2);
}

entry_data_traits::entry_data_traits(const std::vector<std::shared_ptr<field_data_traits>>& fields) :
    fields_(fields)
{
}

int entry_data_traits::compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
{
    for (auto field : fields_)
    {
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
std::vector<uint8_t> entry_data_traits::get_data(const std::span<uint8_t>& entry_span)
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


reference_data_traits::reference_data_traits(std::shared_ptr<entry_data_traits> entry_traits, const std::vector<int> field_references)
{
}

int reference_data_traits::compare(const std::span<uint8_t>& p1, const std::span<uint8_t>& p2)
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

std::vector<uint8_t> reference_data_traits::get_data(const std::span<uint8_t>& entry_span)
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

uint32_t table_row_traits_builder::get_current_offset()
{
    auto sum_sizes = [](int a, std::shared_ptr<field_data_traits> traits) {
        return a + traits->get_size();
        };
    auto offset = std::accumulate(field_traits_.begin(), field_traits_.end(), (uint32_t)0, sum_sizes);

    return offset;
}

int table_row_traits_builder::add_span_field(int size)
{
    auto result = field_traits_.size();
    auto offset = get_current_offset();
    field_traits_.push_back(std::make_shared<span_field>(offset, size));
    return (int)result;
}

int table_row_traits_builder::add_uint32_field()
{
    auto result = field_traits_.size();
    auto offset = get_current_offset();
    field_traits_.push_back(std::make_shared<uint32_field>(offset));
    return (int)result;

}

int table_row_traits_builder::add_int32_field()
{
    auto result = field_traits_.size();
    auto offset = get_current_offset();
    field_traits_.push_back(std::make_shared<int32_field>(offset));
    return (int)result;
}

void table_row_traits_builder::add_key_reference(int position)
{
    if (position < 0 || position >= field_traits_.size())
    {
        throw object_db_exception("key reference does not refer to a known field position");
    }
    field_references_.push_back(position);
}

std::shared_ptr<table_row_traits> table_row_traits_builder::create_table_row_traits()
{
    auto entry_data = std::make_shared<entry_data_traits>(field_traits_);
    return std::make_shared<table_row_traits>(entry_data, field_references_);
}

