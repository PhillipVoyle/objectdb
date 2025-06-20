
#include <fstream>

#include <memory>
#include <sstream>
#include <string>
#include <iomanip>
#include <filesystem>

#include "../include/transaction_log.hpp"
#include "../include/offset_ptr.hpp"
#include "../include/random_access_file.hpp"

std::string calculate_filename(uint64_t filename)
{
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(sizeof(uint16_t) * 2) << std::hex << filename;

    ss << ".dat";

    return ss.str();
}

std::string calculate_file_path(const std::filesystem::path& root_path, uint64_t filename)
{
    auto path = root_path / calculate_filename(filename);
    return path.string();
}


class concrete_file_ref_cache : public file_ref_cache
{
    struct file_entry
    {
        std::shared_ptr<std_random_access_file> file;
        int ref_count;
    };

    std::map<uint64_t, file_entry> cache_;

public:
    std::shared_ptr<random_access_file> acquire_file(uint64_t filename) final
    {
        auto it = cache_.find(filename);
        if (it != cache_.end())
        {
            it->second.ref_count++;
            return it->second.file;
        }
        else
        {
            auto file = std::make_shared<std_random_access_file>(calculate_file_path(std::filesystem::temp_directory_path(), filename));
            cache_[filename] = { file, 1 };
            return file;
        }
    }

    void release_file(uint64_t filename) final
    {
        auto it = cache_.find(filename);
        if (it != cache_.end())
        {
            it->second.ref_count--;
            if (it->second.ref_count <= 0)
            {
                cache_.erase(it);
            }
        }
    }
};


class schema_root
{
};

class transaction_root
{
    far_offset_ptr<schema_root> schema_root_ptr_;
    uint64_t _next_transaction_id = 0;
public:
    explicit transaction_root(filesize_t block_size, filesize_t remaining_space)
    {
    }

    filesize_t get_size() const
    {
        return sizeof(uint64_t) + schema_root_ptr_.get_size();
    }

    void read_from_span(const std::span<uint8_t>& s)
    {
        if (s.size() < get_size())
        {
            throw object_db_exception("transaction_root: span size is less than expected size");
        }
        auto it = s.begin();
        auto it2 = it + sizeof(uint64_t);

        read_uint64({it, it2}, _next_transaction_id);

        it = it2;
        schema_root_ptr_ = far_offset_ptr<schema_root>({ it, it + schema_root_ptr_.get_size() });
    }

    void write_to_span(const std::span<uint8_t>& s) const
    {
        if (s.size() < get_size())
        {
            throw object_db_exception("transaction_root: span size is less than expected size");
        }
        auto it = s.begin();
        auto it2 = it + sizeof(uint64_t);
        write_uint64({ it, it2 }, _next_transaction_id);
        it = it2;
        schema_root_ptr_.write_to_span({ it, it + schema_root_ptr_.get_size() });
    }
};



template<typename file_t>
class concrete_transaction : public transaction
{
    file_t& file_;
public:

    explicit concrete_transaction(file_t& file) : file_(file)
    {
        auto file_size = file_.get_file_size();
        root_node<transaction_root> root(block_size);
        if (file_size == 0)
        {
            root.write_object(0, file_);
        }
        else
        {
            root.read_object(0, file_);
        }
    }

    std::shared_ptr<schema_iterator> create_schema(const std::string& schema_name)
    {
        throw object_db_exception("create_schema not implemented yet");
    }
    std::shared_ptr<table_iterator> create_table(const std::string& schema_name, const table_descriptor& table)
    {
        throw object_db_exception("create_table not implemented yet");
    }
    std::shared_ptr<schema_iterator> get_schema_iterator_start()
    {
        throw object_db_exception("get_schema_iterator_start not implemented yet");
    }
    std::shared_ptr<schema_iterator> get_schema_iterator_end()
    {
        throw object_db_exception("get_schema_iterator_end not implemented yet");
    }
    void commit()
    {
        throw object_db_exception("commit not implemented yet");
    }
    void rollback()
    {
        throw object_db_exception("rollback not implemented yet");
    }
};


template<typename file_t>
class concrete_transaction_log : public transaction_log
{
    std::filesystem::path root_path_;
    file_t file_;
public:
    explicit concrete_transaction_log(const std::filesystem::path& root_path) : root_path_(root_path), file_(calculate_file_path(root_path, 0))
    {
    }

    std::shared_ptr<transaction> begin_transaction() final
    {
        return std::make_shared<concrete_transaction<file_t>>(file_);
    }
};

std::shared_ptr<transaction_log> transaction_log::open(const std::filesystem::path& root_path)
{
    return std::make_shared<concrete_transaction_log<std_random_access_file>>(root_path);
}