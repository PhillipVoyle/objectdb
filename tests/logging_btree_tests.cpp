#include "pch.h"
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <unordered_map>
#include <cstring>
#include <iostream>

#include "../include/logging_btree.hpp"
#include "../include/std_random_access_file.hpp"
#include "../include/memory_random_access_file.hpp"

namespace fs = std::filesystem;

static std::string generate_temp_filename() {
    return (fs::temp_directory_path() / ("gtest_file_" + std::to_string(std::rand()) + ".bin")).string();
}

class printable_logging_btree : public logging_btree
{
public:
    printable_logging_btree(logging_btree_parameters& parms) : logging_btree(parms)
    {
    }

    std::string format_value(const std::span<uint8_t>& value)
    {
        std::string result;
        bool bfirst = true;
        for (auto byte : value)
        {
            if (bfirst)
            {
                bfirst = false;
            }
            else
            {
                result += ", ";
            }
            result += std::to_string(byte);
        }
        return result;
    }

    void print_tree(filesize_t offset, const std::string& prefix = "")
    {
        variable_btree_node node(*this);
        node.read_node(offset);
        std::cout << prefix << "Node at offset: " << offset << ", is_leaf: " << node._is_leaf
            << ", key_size: " << node._key_size
            << ", value_size: " << node._value_size
            << ", data size: " << node._data.size() << std::endl;
        for (int i = 0; i < node.get_value_count(); ++i)
        {
            filesize_t child_offset = 0;
            std::vector<uint8_t> key_at_n(node._key_size);
            std::vector<uint8_t> value_at_n(node._value_size);

            node.get_key_at_n(i, key_at_n);
            node.get_value_at_n(i, value_at_n);

            std::cout << prefix << format_value(key_at_n) << " -> " << format_value(value_at_n) << std::endl;

            if (!node._is_leaf)
            {
                read_filesize(value_at_n, child_offset);
                std::cout << prefix << "Child node offset: " << child_offset << std::endl;
                // Recursively print the child node
                // Note: This is a recursive call, so be careful with large trees to avoid stack overflow
                // You may want to limit the depth of recursion or use an iterative approach for large trees
                print_tree(child_offset, prefix + "   ");
            }
        }
    }
};

class StdRandomAccessFileTest : public ::testing::Test {
protected:
    std::string filename;

    void SetUp() override {
        filename = generate_temp_filename();
    }

    void TearDown() override {
        if (fs::exists(filename)) {
            fs::remove(filename);
        }
    }
};

TEST_F(StdRandomAccessFileTest, InitialFileSizeIsZero) {
    std_random_access_file file(filename);
    EXPECT_EQ(file.get_file_size(), 0);
}

TEST_F(StdRandomAccessFileTest, WriteAndReadSmallData) {
    std_random_access_file file(filename);

    std::vector<uint8_t> write_buf = { 1, 2, 3, 4, 5 };
    file.write_data(0, write_buf);

    std::vector<uint8_t> read_buf(5);
    file.read_data(0, read_buf);
    EXPECT_EQ(read_buf, write_buf);
}

TEST_F(StdRandomAccessFileTest, WriteAtOffsetAndReadBack) {
    std_random_access_file file(filename);

    std::vector<uint8_t> write_buf = { 10, 20, 30, 40 };
    file.write_data(100, write_buf);

    std::vector<uint8_t> read_buf(4);
    file.read_data(100, read_buf);
    EXPECT_EQ(read_buf, write_buf);
}

TEST_F(StdRandomAccessFileTest, FileSizeIncreasesAfterWrite) {
    std_random_access_file file(filename);

    std::vector<uint8_t> write_buf = { 1, 2, 3 };
    file.write_data(50, write_buf);
    EXPECT_GE(file.get_file_size(), 53); // offset + data length
}

class LoggingBTreeTest : public ::testing::Test {
protected:

    memory_random_access_file std_file;
    printable_logging_btree* btree = nullptr;

    void SetUp() override
    {
        logging_btree_parameters params(std_file);
        params.maximum_value_count = 2;
        btree = new printable_logging_btree(params);
    }

    void TearDown() override
    {
        delete btree;
    }
};

TEST_F(LoggingBTreeTest, CreateEmptyRootNode) {
    filesize_t node_size = 0;
    auto root_offset = std_file.get_file_size();
    btree->create_empty_root_node(root_offset, node_size);
    EXPECT_EQ(node_size, 32); // 16 bytes for the header, and 8 bytes for each of the 2 values
    EXPECT_EQ(std_file.get_file_size(), root_offset + node_size);
}

TEST_F(LoggingBTreeTest, VariableBTreeNodeWriteAndRead) {
    variable_btree_node node(*btree);
    node._is_leaf = true;
    node._key_size = 4;
    node._value_size = 8;

    // Create fake data
    node._data = std::vector<uint8_t>(node._key_size + node._value_size, 0xAA);
    node.write_node(0);

    variable_btree_node node2(*btree);
    node2.read_node(0);
    EXPECT_EQ(node2._data, node._data);
}

TEST(CompareSpanTest, BasicComparison) {
    std::vector<uint8_t> a = { 1, 2, 3 };
    std::vector<uint8_t> b = { 1, 2, 3 };
    std::vector<uint8_t> c = { 1, 2, 4 };
    std::vector<uint8_t> d = { 1, 2 };

    EXPECT_EQ(compare_span(a, b), 0);
    EXPECT_LT(compare_span(a, c), 0);
    EXPECT_GT(compare_span(a, d), 0);
}

TEST_F(LoggingBTreeTest, InsertKeyAndData)
{
    filesize_t new_root_offset = 0;
    std::vector<uint8_t> key = { 1, 2, 3, 4 };
    std::vector<uint8_t> data = { 5, 6, 7, 8 };
    auto offset = std_file.get_file_size();
    filesize_t node_size = 0;
    btree->create_empty_root_node(offset, node_size);
    btree->insert_key_and_data(offset, key, data, new_root_offset);
}

TEST_F(LoggingBTreeTest, InsertKeyAndDataAndFindKey)
{
    std::vector<uint8_t> key1 = { 1, 2, 3, 4 };
    std::vector<uint8_t> data1 = { 5, 6, 7, 8 };

    std::vector<uint8_t> key2 = { 2, 3, 4, 5 };
    std::vector<uint8_t> data2 = { 11, 6, 7, 8 };

    std::vector<uint8_t> key3 = { 8, 1, 4, 5 };
    std::vector<uint8_t> data3 = { 77, 2, 7, 99 };

    // this is intended to prefix the entire data set
    std::vector<uint8_t> key4 = { 0, 3, 4, 5 };
    std::vector<uint8_t> data4 = { 12, 2, 7, 99 };

    // this is intended to suffix the entire data set
    std::vector<uint8_t> key5 = { 99, 3, 4, 8 };
    std::vector<uint8_t> data5 = { 88, 2, 2, 2 };

    filesize_t root_offset = std_file.get_file_size();
    filesize_t node_size = 0;
    btree->create_empty_root_node(root_offset, node_size);
    btree->insert_key_and_data(root_offset, key1, data1, root_offset);
    btree->insert_key_and_data(root_offset, key2, data2, root_offset);
    btree->insert_key_and_data(root_offset, key3, data3, root_offset);
    btree->insert_key_and_data(root_offset, key4, data4, root_offset);
    btree->insert_key_and_data(root_offset, key5, data5, root_offset);

    btree->print_tree(root_offset);

    std::vector<uint8_t> value_at_position;
    bool found = false;
    btree->read_value_at_key(root_offset, key1, found, value_at_position);
    EXPECT_EQ(0, compare_span(value_at_position, data1));

    found = false;
    btree->read_value_at_key(root_offset, key2, found, value_at_position);
    EXPECT_EQ(0, compare_span(value_at_position, data2));

    found = false;
    btree->read_value_at_key(root_offset, key3, found, value_at_position);
    EXPECT_EQ(0, compare_span(value_at_position, data3));

    found = false;
    btree->read_value_at_key(root_offset, key4, found, value_at_position);
    EXPECT_EQ(0, compare_span(value_at_position, data4));

    found = false;
    btree->read_value_at_key(root_offset, key5, found, value_at_position);
    EXPECT_EQ(0, compare_span(value_at_position, data5));
}