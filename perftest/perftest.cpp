#include <filesystem>
#include "../include/btree.hpp"
#include "../include/file_allocator.hpp"
#include "../include/file_cache.hpp"
#include "../include/span_iterator.hpp"
#include "../include/table_row_traits.hpp"
#include "../include/file_cache_heap.hpp"

#include <iostream>

void main()
{
    if (std::filesystem::exists("test_cache"))
    {
        std::filesystem::remove_all("test_cache");
    }

    try
    {
        file_cache cache{ "test_cache" };
        file_allocator allocator{ cache };
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
    }
    catch (std::exception ex)
    {
        std::cerr << ex.what() << std::endl;
    }

    if (std::filesystem::exists("test_cache"))
    {
        std::filesystem::remove_all("test_cache");
    }
}