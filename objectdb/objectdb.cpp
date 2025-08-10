#include <filesystem>
#include "../include/btree.hpp"
#include "../include/file_allocator.hpp"
#include "../include/file_cache.hpp"
#include "../include/span_iterator.hpp"
#include "../include/table_row_traits.hpp"

#include <iostream>

void write_path(const btree_iterator& it)
{
    for (int n = 0; n < it.path.size(); n++)
    {
        std::cout << it.path[n].node_offset.get_file_id() << "/" << it.path[n].node_offset.get_offset()
            << " " << it.path[n].btree_position
            << " - " << (it.path[n].is_found ? "found" : "not found")
            << std::endl;
    }
}

void dump_tree_node(btree& bt, file_cache& cache, far_offset_ptr offset, const std::string& padding = "")
{
    auto iterator = cache.get_iterator(offset);
    btree_node node(bt);
    node.read(iterator);

    std::cout << padding << "-- begin node at " << offset.get_file_id() << "/" << offset.get_offset() << std::endl;

    auto is_leaf = node.is_leaf();
    std::cout << padding << "   " << (is_leaf ? "leaf" : "branch") << std::endl;

    auto key_size = node.get_key_size();
    auto value_size = node.get_value_size();
    auto entry_count = node.get_entry_count();
    auto entry_size = key_size + value_size;

    std::cout << padding << "   " << "count " << entry_count << ", key " << key_size << ", value " << value_size << std::endl;

    for (int i = 0; i < entry_count; i ++)
    {
        auto key_vec = node.get_key_at(i);
        auto value_span = node.get_value_at(i);

        std::string key{ key_vec.begin(), key_vec.end() };

        if (is_leaf)
        {
            std::string value{ value_span.begin(), value_span.end() };
            std::cout << padding << "   " << "[" << i << "]" << key << ":" << value << std::endl;
        }
        else
        {
            std::cout << padding << "   " << "[" << i << "]" << key << ":";

            span_iterator it{ value_span };
            far_offset_ptr sub_node_offset;
            sub_node_offset.read(it);

            dump_tree_node(bt, cache, sub_node_offset, padding + "   ");
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
    else
    {
        dump_tree_node(tree, cache, offset);
    }
}


void main()
{
    
    if (std::filesystem::exists("test_cache"))
    {
        std::filesystem::remove_all("test_cache");
    }

    file_cache cache{ "test_cache" };
    file_allocator allocator{ cache };

    auto transaction_id = allocator.create_transaction();

    far_offset_ptr initial{ 0, 0 };

    uint32_t key_size = 700;
    uint32_t value_size = 30;

    auto row_traits_builder = std::make_shared<table_row_traits_builder>();

    int key_id = row_traits_builder->add_span_field(key_size);
    int value_id = row_traits_builder->add_span_field(value_size);

    row_traits_builder->add_key_reference(key_id);

    std::shared_ptr<btree_row_traits> traits = row_traits_builder->create_table_row_traits();


    btree tree{traits, cache, initial, allocator };

    btree_iterator it;
    for (;;)
    {
        write_path(it);
        std::string line;
        std::cout << "btree> " << std::flush;
        if (std::getline(std::cin, line))
        {
            std::stringstream ss(line);
            std::string command;
            ss >> command;

            if (command == "ins" || command == "insert")
            {
                std::string key;
                std::string value;

                ss >> key >> value;
                std::vector<uint8_t> entry(key_size + value_size, 0);
                std::copy_n(key.begin(), std::min(key_size, (uint32_t)key.size()), entry.begin());
                std::copy_n(value.begin(), std::min(value_size, (uint32_t)value.size()), entry.begin() + key_size);

                it = tree.seek_begin({ entry.begin(), key_size });
                if (!it.path.empty() && it.path.back().is_found)
                {
                    std::cerr << "entry already exists" << std::endl;
                }
                else
                {
                    it = tree.insert(transaction_id, it, entry);
                }
            }
            else if (command == "upd" || command == "update")
            {
                std::string key;
                std::string value;

                ss >> key >> value;
                std::vector<uint8_t> entry(key_size + value_size, 0);
                std::copy_n(key.begin(), std::min(key_size, (uint32_t)key.size()), entry.begin());
                std::copy_n(value.begin(), std::min(value_size, (uint32_t)value.size()), entry.begin() + key_size);

                it = tree.seek_begin({ entry.begin(), key_size });
                if (it.path.empty() || !it.path.back().is_found)
                {
                    std::cerr << "no entry for key" << std::endl;
                }
                else
                {
                    it = tree.update(transaction_id, it, entry);
                }
            }
            else if (command == "ups" || command == "upsert")
            {
                std::string key;
                std::string value;

                ss >> key >> value;
                std::vector<uint8_t> entry(key_size + value_size, 0);
                std::copy_n(key.begin(), std::min(key_size, (uint32_t)key.size()), entry.begin());
                std::copy_n(value.begin(), std::min(value_size, (uint32_t)value.size()), entry.begin() + key_size);

                it = tree.upsert(transaction_id, entry);
            }
            else if (command == "sek" || command == "seek")
            {
                std::string key;

                ss >> key;
                std::vector<uint8_t> blob(key_size, 0);
                std::copy_n(key.begin(), std::min(key_size, (uint32_t)key.size()), blob.begin());
                it = tree.seek_begin(blob);

                if (it.path.empty() || !it.path.back().is_found)
                {
                    std::cerr << "no entry at key" << std::endl;
                }
                else
                {
                    auto entry = tree.get_entry(it);
                    std::string value(entry.begin() + key_size, entry.end());

                    std::cout << key << "=" << value << std::endl;
                }
            }
            else if (command == "del" || command == "delete")
            {
                std::string key;

                ss >> key;
                std::vector<uint8_t> blob(key_size, 0);
                std::copy_n(key.begin(), std::min(key_size, (uint32_t)key.size()), blob.begin());
                it = tree.seek_begin(blob);

                if (it.path.empty() || !it.path.back().is_found)
                {
                    std::cerr << "no entry at key" << std::endl;
                }
                else
                {
                    it = tree.remove(transaction_id, it);
                }
            }
            else if (command == "xit" || command == "exit")
            {
                break;
            }
            else if (command == "nxt" || command == "next")
            {
                it = tree.next(it);
            }
            else if (command == "prv" || command == "previous")
            {
                it = tree.prev(it);
            }
            else if (command == "end")
            {
                it = tree.end();
            }
            else if (command == "beg" || command == "begin")
            {
                it = tree.begin();
            }
            else if (command == "dmp" || command == "dump")
            {
                dump_tree(cache, tree);
            }
            else if (command == "shw" || command == "show")
            {
                if (it.path.empty() || !it.path.back().is_found)
                {
                    std::cerr << "no entry" << std::endl;
                }
                else
                {
                    auto entry = tree.get_entry(it);
                    std::string key{ entry.begin(), entry.begin() + key_size };
                    std::string value{ entry.begin() + key_size, entry.begin() + key_size + value_size };

                    std::cout << key << "=" << value << std::endl;
                }
            }
            else
            {
                std::cerr << "unrecognised command: " << command << std::endl;
            }
        }
        else
        {
            break;
        }

    }
    std::cout << "bye";
}