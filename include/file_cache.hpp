
#pragma once

#include <map>
#include <list>
#include <fstream>
#include <filesystem>
#include <tuple>

#include "../include/core.hpp"
#include "../include/file_iterator.hpp"

class far_offset_ptr;

class block_cache
{
    class block_cache_entry
    {
    public:
        std::shared_ptr<std::vector<uint8_t>> block;
        std::list<std::tuple<filesize_t, filesize_t>>::iterator lru_iterator;
    };

    int lru_max_;
    std::map<std::tuple<filesize_t, filesize_t>, block_cache_entry> blocks_;
    std::list<std::tuple<filesize_t, filesize_t>> lru_block_list_;

    block_cache() = delete;
public:
    block_cache(int lru_size);
    std::shared_ptr<std::vector<uint8_t>> get_block(filesize_t filename, filesize_t offset);
    bool exists(filesize_t filename, filesize_t offset);
};

class file_cache
{
public:
    virtual ~file_cache() = default;
    file_cache() = default;
    // Delete copy constructor and copy assignment operator
    file_cache(const file_cache&) = delete;
    file_cache& operator=(const file_cache&) = delete;

    virtual filesize_t get_file_size(filesize_t file_id) = 0;
    virtual void write(filesize_t file_id, filesize_t offset, uint8_t data) = 0;
    virtual uint8_t read(filesize_t file_id, filesize_t offset) = 0;

    virtual void write_bytes(filesize_t file_id, filesize_t offset, std::span<const uint8_t> data) = 0;
    virtual void read_bytes(filesize_t file_id, filesize_t offset, std::span<uint8_t> data) = 0;

    virtual file_iterator get_iterator(filesize_t file_id, filesize_t offset = 0) = 0;
    virtual file_iterator get_iterator(const far_offset_ptr& ptr) = 0;
};

class concrete_file_cache : public file_cache
{  
    std::filesystem::path cache_path; // Use the alias 'fs::path' to resolve incomplete type error  
    std::map<filesize_t, std::fstream> file_streams; // Map to hold open file streams
    std::list<filesize_t> lru_file_list;

    block_cache blocks_ = block_cache{ 4096 };

    void evict_file_if_needed(); // Evict the least recently used file if needed
    std::fstream& get_stream(filesize_t file_id, std::ios::openmode mode);
    static std::string get_filename(const std::filesystem::path& cache_path, filesize_t file_id);


public:
    concrete_file_cache(const std::filesystem::path& path) : cache_path(path) {}
    ~concrete_file_cache() = default;

    // Delete copy constructor and copy assignment operator
    concrete_file_cache(const concrete_file_cache&) = delete;
    concrete_file_cache& operator=(const concrete_file_cache&) = delete;

    filesize_t get_file_size(filesize_t file_id) override;
    void write(filesize_t file_id, filesize_t offset, uint8_t data) override;
    uint8_t read(filesize_t file_id, filesize_t offset) override;

    void write_bytes(filesize_t file_id, filesize_t offset, std::span<const uint8_t> data) override;
    void read_bytes(filesize_t file_id, filesize_t offset, std::span<uint8_t> data) override;

    file_iterator get_iterator(filesize_t file_id, filesize_t offset = 0) override;
    file_iterator get_iterator(const far_offset_ptr& ptr) override;
};
