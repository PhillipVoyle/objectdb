#pragma once

#include "../include/field_descriptor.hpp"

enum index_type
{
    primary_key,
    unique_key,
    foreign_key,
    index,
    reference_key
};

struct index_descriptor
{
    index_type type;
    std::string name;
    std::vector<int> local_field_references;
    int remote_schema;
    int remote_table_name;
    std::vector<int> remote_schema_field_references;
};

struct table_descriptor
{
    std::vector<field_descriptor> fields;
    std::vector<index_descriptor> indexes;

    filesize_t get_row_size() const;

    const index_descriptor& get_primary_key() const;
    filesize_t get_primary_key_size() const;
};
