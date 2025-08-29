
#include "pch.h"
#include <filesystem>
#include "../include/btree.hpp"
#include "../include/file_allocator.hpp"
#include "../include/file_cache.hpp"
#include "../include/span_iterator.hpp"
#include "../include/table_row_traits.hpp"
#include "../include/file_cache_heap.hpp"

class btree_test_fixture: public ::testing::Test
{
public:
    void clear()
    {
        if (std::filesystem::exists("test_cache"))
        {
            std::filesystem::remove_all("test_cache");
        }
    }

    virtual void SetUp()
    {
        clear();
    }

    virtual void TearDown()
    {
        clear();
    }
};

TEST_F(btree_test_fixture, test_insert_update_read_delete)
{
    concrete_file_cache cache{ "test_cache" };
    concrete_file_allocator allocator{ cache };
    file_cache_heap heap{ allocator };

    auto transaction_id = allocator.create_transaction();

    far_offset_ptr initial{ 0, 0 };

    uint32_t key_size = 500;
    uint32_t value_size = 500;

    auto row_traits_builder = std::make_shared<table_row_traits_builder>();

    int key_id = row_traits_builder->add_span_field(key_size);
    int value_id = row_traits_builder->add_span_field(value_size);

    row_traits_builder->add_key_reference(key_id);

    std::shared_ptr<btree_row_traits> traits = row_traits_builder->create_table_row_traits();

    btree tree(traits, cache, initial, allocator, heap);

    std::vector<uint8_t> entry(key_size + value_size, 0);


    for (uint32_t i = 0; i < 100; ++i)
    {
        span_iterator key_span{ {entry.begin(), key_size} };
        span_iterator value_span{ {entry.begin() + key_size, value_size} };

        write_uint32(key_span, i);
        write_uint32(value_span, i % 10);
        tree.upsert(transaction_id, entry);
    }

    std::vector<uint8_t> key(key_size);
    for (uint32_t i = 0; i < 100; ++i)
    {
        span_iterator key_span{ {key.begin(), key_size} };
        write_uint32(key_span, i);

        auto it = tree.seek_begin(key);
        if (!it.path.empty() && it.path.back().is_found)
        {
            tree.remove(transaction_id, it);
        }
    }
    EXPECT_TRUE(tree.begin() == tree.end());
};

TEST_F(btree_test_fixture, test_delete)
{

    concrete_file_cache cache{ "test_cache" };
    concrete_file_allocator allocator{ cache };
    file_cache_heap heap{ allocator };

    auto transaction_id = allocator.create_transaction();

    far_offset_ptr initial{ 0, 0 };

    uint32_t key_size = 700;
    uint32_t value_size = 30;

    auto row_traits_builder = std::make_shared<table_row_traits_builder>();

    int key_id = row_traits_builder->add_span_field(key_size);
    int value_id = row_traits_builder->add_span_field(value_size);

    row_traits_builder->add_key_reference(key_id);

    std::shared_ptr<btree_row_traits> traits = row_traits_builder->create_table_row_traits();
    btree tree(traits, cache, initial, allocator, heap);

    std::vector<std::string> test_strings = { "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq", "rr", "ss", "tt", "uu", "vv"};

    for (auto s : test_strings)
    {
        std::vector<uint8_t> entry(key_size + value_size, 0);
        std::copy(s.begin(), s.begin() + s.size(), entry.begin());
        std::copy(s.begin(), s.begin() + s.size(), entry.begin() + key_size);

        tree.upsert(transaction_id, entry);
    }

    std::string remove_string = "ee";
    std::vector<uint8_t> key(key_size);
    std::copy(remove_string.begin(), remove_string.begin() + remove_string.size(), key.begin());

    auto it = tree.seek_begin(key);
    EXPECT_FALSE(it == tree.end());
    EXPECT_FALSE(!it.btree_offset);
    EXPECT_FALSE(it.path.empty());
    EXPECT_TRUE(it.path.back().is_found);

    tree.remove(transaction_id, it);

    std::vector<std::string> remaining;

    it = tree.begin();
    for(auto s : test_strings)
    {

        std::vector<uint8_t> key(key_size);
        std::copy(s.begin(), s.begin() + s.size(), key.begin());
        auto it = tree.seek_begin(key);
        EXPECT_FALSE(it == tree.end());
        EXPECT_FALSE(!it.btree_offset);
        EXPECT_FALSE(it.path.empty());

        if (s == remove_string)
        {
            EXPECT_FALSE(it.path.back().is_found);
        }
        else
        {
            EXPECT_TRUE(it.path.back().is_found);
        }
    }
}
