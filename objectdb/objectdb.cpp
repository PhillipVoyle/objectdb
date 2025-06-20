#include <iostream>
#include <filesystem>
#include <map>
#include <fstream>
#include <cassert>

#include "../include/core.hpp"
#include "../include/transaction_log.hpp"
#include "../include/index_node.hpp"
#include "../include/file_cache.hpp"

int main()
{
    std::cout << "phillip voyle's object db" << std::endl;

    /*
    auto log = transaction_log::open(std::filesystem::temp_directory_path());

    auto transaction = log->begin_transaction();
    transaction->create_schema("test_schema");

    table_descriptor table_desc;
    table_desc.fields.push_back(create_integer_field("id", 12));
    table_desc.fields.push_back(create_text_field("name", 64));
    table_desc.fields.push_back(create_binary_field("data", 128));

    auto table = transaction->create_table("test_schema", table_desc);
    std::cout << "Created table with fields:" << std::endl;
    */

    auto path = std::filesystem::current_path();
    auto db_path = path / "db";
    std::filesystem::create_directories(db_path);
    file_cache cache(db_path);


    return 0;
}
