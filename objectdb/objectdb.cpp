#include <filesystem>
#include "../include/btree.hpp"
#include "../include/file_allocator.hpp"
#include "../include/file_cache.hpp"
#include "../include/span_iterator.hpp"

#include <iostream>

void write_path(const btree_iterator& it)
{
    for (int n = 0; n < it.path.size(); n++)
    {
        std::cout << it.path[n].node_offset.get_file_id() << "/" << it.path[n].node_offset.get_offset() << " " << it.path[n].btree_position << " - " << (it.path[n].is_found ? "found" : "not found") << std::endl;
    }
}

void dump_tree_node(file_cache& cache, far_offset_ptr offset, const std::string& padding = "")
{
    auto iterator = cache.get_iterator(offset);
    btree_node node;
    node.read(iterator);

    std::cout << padding << "-- begin node at " << offset.get_file_id() << "/" << offset.get_offset() << std::endl;

    auto is_leaf = node.is_leaf();
    std::cout << padding << "   " << (is_leaf ? "leaf" : "branch") << std::endl;

    auto metadata = node.get_metadata();
    std::cout << padding << "   " << "count " << metadata.entry_count << ", key " << metadata.key_size << ", value " << metadata.value_size << std::endl;

    for (int i = 0; i < metadata.entry_count; i ++)
    {
        auto key_span = node.get_key_at(i);
        std::vector<uint8_t> key_bytes{ key_span.begin(), key_span.end() };

        auto value_span = node.get_value_at(i);
        std::vector<uint8_t> value_bytes{ value_span.begin(), value_span.end() };

        span_iterator key_it{ key_bytes };
        auto key = read_uint32(key_it);


        if (is_leaf)
        {
            span_iterator value_it{ value_bytes };
            auto value = read_uint32(value_it);
            std::cout << padding << "   " << "[" << i << "]" << key << ":" << value << std::endl;
        }
        else
        {
            std::cout << padding << "   " << "[" << i << "]" << key << ":";

            span_iterator it{ value_bytes };
            far_offset_ptr sub_node_offset;
            sub_node_offset.read(it);

            dump_tree_node(cache, sub_node_offset, padding + "   ");
        }
    }

    std::cout << padding << "-- end" << std::endl;
}

void dump_tree(file_cache& cache, btree& tree)
{
    auto offset = tree.get_offset();
    if (offset.get_file_id() == 0 && offset.get_offset() == 0)
    {
        std::cout << "empty btree" << std::endl;
    }
    dump_tree_node(cache, offset);
}

void main()
{
    if (std::filesystem::exists("test_cache"))
    {
        std::filesystem::remove_all("test_cache");
    }

    file_cache cache{ "test_cache" };
    file_allocator allocator{ cache };

    auto transaction_id = 0; // allocator.create_transaction();

    far_offset_ptr initial{ 0, 0 };

    uint32_t key_size = 800;
    uint32_t value_size = 16;

    btree tree{ cache, initial, allocator, key_size, value_size };
    dump_tree(cache, tree);

    std::vector<uint8_t> entry(key_size + value_size, 0);

    transaction_id = allocator.create_transaction();
    for (uint32_t i = 10; i < 20; ++i)
    {
        std::cout << "Inserting key: " << i << ", value: " << (i % 10) << std::endl;

        span_iterator key_span{ {entry.begin(), key_size} };
        span_iterator value_span{ {entry.begin() + key_size, value_size} };

        write_uint32(key_span, i);
        uint32_t value = i % 21;
        write_uint32(value_span, value);
        if (value == 0)
        {
            transaction_id = allocator.create_transaction();
        }

        std::cout << "Seek key: " << i << ", value: " << value << std::endl;
        auto it_seek = tree.seek_begin({ entry.begin(), key_size });
        write_path(it_seek);
        auto it = tree.upsert(transaction_id, { entry.begin(), key_size }, { entry.begin() + key_size, value_size });


        std::cout << "Inserted key: " << i << ", value: " << value << std::endl;
        write_path(it);

        /*
        std::cout << "Seek key (post): " << i << ", value: " << (i % 10) << std::endl;
        auto it_seek_post = tree.seek_begin({ entry.begin(), key_size });
        write_path(it_seek_post);
        std::cout << std::endl;
        */

    }
    dump_tree(cache, tree);

    span_iterator key_span{ {entry.begin(), key_size} };
    span_iterator value_span{ {entry.begin() + key_size, value_size} };

    int key = 3;
    write_uint32(key_span, key);
    uint32_t value = 88;
    write_uint32(value_span, value);

    // provoke an update
    auto it_update = tree.upsert(transaction_id, { entry.begin(), key_size }, { entry.begin() + key_size, value_size });
    write_path(it_update);
    dump_tree(cache, tree);

    tree.remove(transaction_id, it_update);
    dump_tree(cache, tree);
}