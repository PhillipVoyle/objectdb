#include "pch.h"
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <unordered_map>
#include <cstring>

#include "../include/logging_btree.hpp"
#include "../include/std_random_access_file.hpp"
#include "../include/memory_random_access_file.hpp"

namespace fs = std::filesystem;

static std::string generate_temp_filename() {
    return (fs::temp_directory_path() / ("gtest_file_" + std::to_string(std::rand()) + ".bin")).string();
}

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
    EXPECT_TRUE(file.write_data(0, write_buf));

    std::vector<uint8_t> read_buf(5);
    std::span<uint8_t> read_span(read_buf);
    EXPECT_TRUE(file.read_data(0, read_span));
    EXPECT_EQ(read_buf, write_buf);
}

TEST_F(StdRandomAccessFileTest, WriteAtOffsetAndReadBack) {
    std_random_access_file file(filename);

    std::vector<uint8_t> write_buf = { 10, 20, 30, 40 };
    EXPECT_TRUE(file.write_data(100, write_buf));

    std::vector<uint8_t> read_buf(4);
    std::span<uint8_t> read_span(read_buf);
    EXPECT_TRUE(file.read_data(100, read_span));
    EXPECT_EQ(read_buf, write_buf);
}

TEST_F(StdRandomAccessFileTest, FileSizeIncreasesAfterWrite) {
    std_random_access_file file(filename);

    std::vector<uint8_t> write_buf = { 1, 2, 3 };
    EXPECT_TRUE(file.write_data(50, write_buf));
    EXPECT_GE(file.get_file_size(), 53); // offset + data length
}

class LoggingBTreeTest : public ::testing::Test {
protected:

    memory_random_access_file std_file;
    logging_btree* btree = nullptr;

    void SetUp() override
    {
        logging_btree_parameters params(std_file);
        params.maximum_value_count = 2;
        btree = new logging_btree(params);
    }

    void TearDown() override
    {
        delete btree;
    }
};

TEST_F(LoggingBTreeTest, CreateEmptyRootNode) {
    filesize_t node_size = 0;
    bool result = btree->create_empty_root_node(0, node_size);
    EXPECT_TRUE(result);
    EXPECT_EQ(node_size, 16);
    EXPECT_EQ(std_file.get_file_size(), 16);
}

TEST_F(LoggingBTreeTest, VariableBTreeNodeWriteAndRead) {
    variable_btree_node node(*btree);
    node._is_leaf = true;
    node._key_size = 4;
    node._value_size = 8;

    // Create fake data
    node._data = std::vector<uint8_t>(node._key_size + node._value_size, 0xAA);
    EXPECT_TRUE(node.write_node(0));

    variable_btree_node node2(*btree);
    EXPECT_TRUE(node2.read_node(0));
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
    auto create_result = btree->create_empty_root_node(offset, node_size);
    EXPECT_TRUE(create_result);

    bool result = btree->insert_key_and_data(offset, key, data, new_root_offset);
    EXPECT_TRUE(result);
}


TEST_F(LoggingBTreeTest, InsertKeyAndDataAndFindKey)
{
    filesize_t new_root_offset = 0;
    std::vector<uint8_t> key1 = { 1, 2, 3, 4 };
    std::vector<uint8_t> data1 = { 5, 6, 7, 8 };

    std::vector<uint8_t> key2 = { 2, 3, 4, 5 };
    std::vector<uint8_t> data2 = { 11, 6, 7, 8 };

    std::vector<uint8_t> key3 = { 8, 1, 4, 5 };
    std::vector<uint8_t> data3 = { 77, 2, 7, 99 };

    auto offset = std_file.get_file_size();
    filesize_t node_size = 0;
    auto create_result = btree->create_empty_root_node(offset, node_size);
    EXPECT_TRUE(create_result);

    bool result = btree->insert_key_and_data(offset, key1, data1, new_root_offset);
    EXPECT_TRUE(result);
    result = btree->insert_key_and_data(new_root_offset, key2, data2, new_root_offset);
    EXPECT_TRUE(result);
    result = btree->insert_key_and_data(new_root_offset, key3, data3, new_root_offset);
    EXPECT_TRUE(result);
    value_location location;
    result = btree->find_key(new_root_offset, key2, location);
    EXPECT_TRUE(result);
    EXPECT_EQ(location.best_value_position.size(), 1);
}