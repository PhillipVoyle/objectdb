#include "../include/table_row_traits.hpp"

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
