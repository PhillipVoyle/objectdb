
#include "../include/table_descriptor.hpp"

filesize_t table_descriptor::get_row_size() const
{
    filesize_t size = 0;
    for (const auto& field : fields)
    {
        size += field.get_size();
    }
    return size;
}

const index_descriptor& table_descriptor::get_primary_key() const
{
    // find the primary key in the list of indexes
    for (const auto& index : indexes)
    {
        if (index.type == index_type::primary_key)
        {
            return index;
        }
    }
    throw std::runtime_error("Primary key not found in table descriptor.");
}

filesize_t table_descriptor::get_primary_key_size() const
{
    auto primary_key = get_primary_key();
    filesize_t size = 0;

    for (const auto& field : primary_key.local_field_references)
    {
        if (field >= fields.size())
        {
            throw std::runtime_error("Invalid field reference in primary key.");
        }
        size_t field_size = fields[field].get_size();
        if (field_size == 0)
        {
            throw std::runtime_error("Field size cannot be zero in primary key.");
        }
        size += field_size;
    }
    return size;
}
