
#include "pch.h"
#include <filesystem>
#include "../include/btree.hpp"
#include "../include/file_allocator.hpp"
#include "../include/file_cache.hpp"
#include "../include/span_iterator.hpp"

TEST(btree_tests, test_insert_update_read_delete)
{
    LexicalComparitor comparitor;
    if (std::filesystem::exists("test_cache"))
    {
        std::filesystem::remove_all("test_cache");
    }

    file_cache cache{ "test_cache" };
    file_allocator allocator{ cache };

    auto transaction_id = allocator.create_transaction();

    far_offset_ptr initial{ 0, 0 };

    uint32_t key_size = 500;
    uint32_t value_size = 500;

    btree tree{ cache, initial, allocator, key_size, value_size };

    std::vector<uint8_t> entry(key_size + value_size, 0);


    for (uint32_t i = 0; i < 100; ++i)
    {
        span_iterator key_span{ {entry.begin(), key_size} };
        span_iterator value_span{ {entry.begin() + key_size, value_size} };

        write_uint32(key_span, i);
        write_uint32(value_span, i % 10);
        tree.upsert(transaction_id, { entry.begin(), key_size }, { entry.begin() + key_size, value_size }, comparitor);
    }
}