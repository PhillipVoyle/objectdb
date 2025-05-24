#pragma once

#include <string>
#include <memory>
#include <vector>

#include "../include/core.hpp"

struct index_descriptor
{
    std::string name;
    std::vector<int> field_references;
};

enum field_type
{
    ft_integer,
    ft_bool,
    ft_text,
    ft_binary,
    ft_date,
    ft_real
};

struct field_descriptor
{
    std::string name;
    uint64_t type_descriptor;
};

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
};

struct field_adjustment
{
    std::string old_name;
    std::string new_name;
    uint64_t new_type;
};

struct index_adjustment
{
    std::string old_name;
    std::string new_name;
    std::vector<int> new_local_field_references;
};

struct alter_table_command
{
    std::string new_name;
    std::vector<field_adjustment> field_adjustments;
    std::vector<index_adjustment> index_adjustments;
};

class row_iterator
{
public:
    virtual ~row_iterator() = default;
    // index operations
    virtual blob_t get_key() = 0;
    virtual blob_t get_value() = 0;

    virtual void insert_entry(const span_t& key, const span_t& value) = 0;
    virtual void update_entry(const span_t& value) = 0;
    virtual void delete_entry() = 0;

    // iterating through indexes
    virtual void seek_forward(const std::string& seek_to) = 0;
    virtual void seek_backward(const std::string& seek_to) = 0;

    virtual bool found() = 0;
    virtual bool at_end() = 0;
    virtual bool at_start() = 0;

    virtual void step_forward() = 0;
    virtual void step_back() = 0;
};


class index_iterator
{
public:
    virtual ~index_iterator() = default;

    // index operations
    virtual const index_descriptor& get_descriptor() = 0;
    virtual void alter(const index_descriptor& descriptor) = 0;

    // iterating through indexes
    virtual void seek_forward(const std::string& seek_to) = 0;
    virtual void seek_backward(const std::string& seek_to) = 0;

    virtual bool found() = 0;
    virtual bool at_end() = 0;
    virtual bool at_start() = 0;

    virtual void step_forward() = 0;
    virtual void step_back() = 0;

    virtual void drop() = 0;
};


class table_iterator
{
public:
    virtual ~table_iterator() = default;
    virtual const std::string get_table_name() = 0;

    virtual void create(const table_descriptor& descriptor) = 0;
    virtual const table_descriptor& get_descriptor() = 0;
    virtual void alter(const alter_table_command& descriptor) = 0;

    virtual std::shared_ptr<index_iterator> get_index_iterator(const std::string& index) = 0;

    virtual void insert_row(const span_t& values) = 0;
    virtual void update_row(const span_t& key, const span_t& values) = 0;
    virtual void delete_row(const span_t& key) = 0;


    // iterating through tables
    virtual void seek_forward(const std::string& seek_to) = 0;
    virtual void seek_backward(const std::string& seek_to) = 0;

    virtual bool found() = 0;
    virtual bool at_end() = 0;
    virtual bool at_start() = 0;

    virtual void step_forward() = 0;
    virtual void step_back() = 0;

    virtual void drop() = 0;
};


class schema_iterator
{
public:
    virtual ~schema_iterator() = default;
    virtual const std::string& get_schema_name() = 0;

    virtual void create_schema(const std::string& schema_name) = 0;

    virtual std::shared_ptr<table_iterator> create_table(const table_descriptor& descriptor) = 0;
    virtual std::shared_ptr<table_iterator> get_table_iterator(const std::string& table_name) = 0;

    // iterating through schemas
    virtual void seek_forward(const std::string& seek_to) = 0;
    virtual void seek_backward(const std::string& seek_to) = 0;

    virtual bool found() = 0;
    virtual bool at_end() = 0;
    virtual bool at_start() = 0;

    virtual void step_forward() = 0;
    virtual void step_back() = 0;

    virtual void drop() = 0;
};

class transaction
{
public:
    virtual ~transaction() = default;

    virtual std::shared_ptr<schema_iterator> create_schema(const std::string& schema_name) = 0;
    virtual std::shared_ptr<table_iterator> create_table(const std::string& schema_name, const table_descriptor& table) = 0;

    virtual std::shared_ptr<schema_iterator> get_schema_iterator_start() = 0;
    virtual std::shared_ptr<schema_iterator> get_schema_iterator_end() = 0;

    virtual void commit() = 0;
    virtual void rollback() = 0;
};

class transaction_log
{
public:
    virtual ~transaction_log() = default;
    virtual std::shared_ptr<transaction> begin_transaction();
};