#include <memory>
#include "flat_file.hpp"
#include <exception>

class btree
{
    flat_file& _file;
    int _block_size;
    int _degree;
    int _key_size;
    int _value_size;
    int _offset_size = 8;

    class btree_node
    {
        const int preamble_size = 4;

        btree* _btr;
    public:
        std::vector<uint8_t> data;
        explicit btree_node(btree* btr) :_btr(btr)
        {
            assert(btr != nullptr);
            data.resize(btr->_block_size);
        }

        int write(filesize_t block_number, flat_file& file)
        {
            assert(file.get_blocksize() == data.size());
            return file.write_block(block_number, data);
        }

        int read(filesize_t block_number, flat_file& file)
        {
            assert(file.get_blocksize() == data.size());
            return file.read_block(block_number, data);
        }

        int get_key_count()
        {
            uint16_t result = 0;
            result = (((uint16_t)data[0]) << 8) | ((uint16_t)data[1]);

            return result;
        }

        void set_key_count(int count)
        {
            data[0] = (count >> 8) & 0xFF;
            data[1] = count & 0xFF;
        }

        bool is_leaf()
        {
            return ((std::byte)(data[2]) & (std::byte)0x1) == (std::byte)0;
        }

        void get_key_at_n(int n, std::vector<uint8_t>& key)
        {
            // check preconditions
            assert(get_key_count() > 0);
            assert(n < get_key_count());

            int use_value_size = is_leaf()
                ? _btr->_value_size
                : _btr->_offset_size;
            int use_entry_size = _btr->_key_size + use_value_size;

            auto data_it = data.begin() + preamble_size + (use_entry_size * n);

            key.resize(_btr->_key_size);
            std::copy_n(data_it, _btr->_key_size, key.begin());
        }

        void get_value_at_n(int n, std::vector<uint8_t>& value)
        {
            // check preconditions
            assert(get_key_count() > 0);
            assert(n < get_key_count());

            int use_value_size = is_leaf()
                ? _btr->_value_size
                : _btr->_offset_size;
            int use_entry_size = _btr->_key_size + use_value_size;

            auto data_it = data.begin() + preamble_size + (use_entry_size * n) + _btr->_key_size;

            value.resize(use_value_size);
            std::copy_n(data_it, use_value_size, value.begin());
        }

        void get_nth_item(int n, std::vector<uint8_t>& key, std::vector<uint8_t>* value = nullptr)
        {
            get_key_at_n(n, key);

            if (value != nullptr)
            {
                get_value_at_n(n, *value);
            }
        }

        int find_insert_position(const std::vector<uint8_t>& key, bool& is_update)
        {
            int key_count = get_key_count();
            std::vector<uint8_t> existing_key;
            int insert_pos = 0;

            while (insert_pos < key_count)
            {
                get_key_at_n(insert_pos, existing_key);
                if (existing_key >= key) break;
                ++insert_pos;
            }

            is_update = (insert_pos < key_count && existing_key == key);
            return insert_pos;
        }

        void insert_at_position(int pos, const std::vector<uint8_t>& key, const std::vector<uint8_t>& value)
        {
            int key_count = get_key_count();
            int use_value_size = is_leaf() ? _btr->_value_size : _btr->_offset_size;
            int use_entry_size = _btr->_key_size + use_value_size;

            auto dest_it = data.begin() + preamble_size + (use_entry_size * (pos + 1));
            auto src_it = data.begin() + preamble_size + (use_entry_size * pos);
            std::move_backward(src_it, data.begin() + preamble_size + (use_entry_size * key_count), dest_it);

            std::copy(key.begin(), key.end(), src_it);
            std::copy(value.begin(), value.end(), src_it + _btr->_key_size);
            set_key_count(key_count + 1);
        }

        void update_at_position(int pos, const std::vector<uint8_t>& value)
        {
            int use_value_size = is_leaf() ? _btr->_value_size : _btr->_offset_size;
            int use_entry_size = _btr->_key_size + use_value_size;
            auto value_it = data.begin() + preamble_size + (use_entry_size * pos) + _btr->_key_size;
            std::copy(value.begin(), value.end(), value_it);
        }

        void upsert_item(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value)
        {
            bool is_update;
            int pos = find_insert_position(key, is_update);

            if (is_update)
            {
                update_at_position(pos, value);
            }
            else
            {
                int max_keys = btree::calculate_max_keys(_btr->_block_size, _btr->_key_size, is_leaf() ? _btr->_value_size : _btr->_offset_size);
                if (get_key_count() >= max_keys) {
                    throw std::overflow_error("Node is full, needs to be split");
                }
                insert_at_position(pos, key, value);
            }
        }

        void split(btree_node& new_node, std::vector<uint8_t>& mid_key)
        {
            int key_count = get_key_count();
            int mid_index = key_count / 2;
            int use_value_size = is_leaf() ? _btr->_value_size : _btr->_offset_size;
            int use_entry_size = _btr->_key_size + use_value_size;

            get_key_at_n(mid_index, mid_key);

            new_node.set_key_count(key_count - mid_index - 1);
            set_key_count(mid_index);

            auto new_data_it = new_node.data.begin() + preamble_size;
            auto src_data_it = data.begin() + preamble_size + ((mid_index + 1) * use_entry_size);
            std::copy(src_data_it, data.begin() + preamble_size + (key_count * use_entry_size), new_data_it);
        }

        void get_min(std::vector<uint8_t>& key, std::vector<uint8_t>* data_out = nullptr)
        {
            return get_nth_item(0, key, data_out);
        }
    };

    static int calculate_max_keys(int block_size, int key_size, int value_size)
    {
        int max_keys = (block_size - 4) / (key_size + value_size);
        return max_keys;
    }

    static int calculate_degree(int block_size, int key_size, int value_size)
    {
        int max_keys = calculate_max_keys(block_size, key_size, value_size);

        if (max_keys < 10)
        {
            throw std::runtime_error("Node cannot fit at least 10 keys in a block");
        }
        return (max_keys + 1) / 2;
    }

    filesize_t get_root_block()
    {
        return _file.get_file_size() / _block_size - 1;
    }

    filesize_t find_leaf_node(const std::vector<uint8_t>& key)
    {
        filesize_t block = get_root_block();
        btree_node node(this);
        node.read(block, _file);

        while (!node.is_leaf()) {
            bool is_update;
            int pos = node.find_insert_position(key, is_update);
            std::copy_n(node.data.begin() + 4 + pos * _offset_size, _offset_size, reinterpret_cast<uint8_t*>(&block));
            node.read(block, _file);
        }

        return block;
    }

public:

    class btree_seek_result
    {
    public:
        filesize_t block_id;
        int position;
        bool exists;
        bool insert_requires_split;
    };

    btree(flat_file& storage, int key_size, int value_size) : _file(storage)
    {
        _key_size = key_size;
        _value_size = value_size;
    }

    void insert_new_key(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value)
    {
        filesize_t leaf_block = find_leaf_node(key);
        btree_node node(this);
        node.read(leaf_block, _file);

        bool is_update;
        int insert_pos = node.find_insert_position(key, is_update);
        if (is_update) {
            throw std::runtime_error("Key already exists");
        }

        int key_count = node.get_key_count();
        if (key_count >= (2 * _degree - 1)) {
            btree_node new_sibling(this);
            std::vector<uint8_t> mid_key;
            node.split(new_sibling, mid_key);

            filesize_t new_block = _file.get_file_size() / _block_size;
            new_sibling.write(new_block, _file);
            node.write(leaf_block, _file);

            insert_new_key(key, value);
        }
        else {
            int use_value_size = _value_size;
            int use_entry_size = _key_size + use_value_size;

            auto insert_it = node.data.begin() + 4 + (insert_pos * use_entry_size);
            node.data.insert(insert_it, key.begin(), key.end());
            node.data.insert(insert_it + _key_size, value.begin(), value.end());

            node.set_key_count(key_count + 1);
            node.write(leaf_block, _file);
        }
    }


    void update_existing_key(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value)
    {
        // TODO: Implement updating an existing key's value in the leaf node
    }

    std::vector<uint8_t> find_value_for_key(const std::vector<uint8_t>& key)
    {
        // TODO: Implement searching for a key and returning its associated value
        return {};
    }

    void upsert(const std::vector<uint8_t>& key, const std::vector<uint8_t>& value)
    {
        if (find_value_for_key(key).empty()) {
            insert_new_key(key, value);
        }
        else {
            update_existing_key(key, value);
        }
    }
};

