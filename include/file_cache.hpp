
#pragma once

#include <map>
#include <list>
#include <fstream>
#include <filesystem>

#include "../include/core.hpp"
#include "../include/file_iterator.hpp"

class far_offset_ptr;

class file_cache  
{  
    std::filesystem::path cache_path; // Use the alias 'fs::path' to resolve incomplete type error  
    std::map<filesize_t, std::fstream> file_streams; // Map to hold open file streams
    std::list<filesize_t> lru_list;

    void evict_if_needed(); // Evict the least recently used file if needed
    std::fstream& get_stream(filesize_t file_id, std::ios::openmode mode);
    static std::string get_filename(const std::filesystem::path& cache_path, filesize_t file_id);

public:
    file_cache(const std::filesystem::path& path) : cache_path(path) {}
    ~file_cache() = default;

    // Delete copy constructor and copy assignment operator
    file_cache(const file_cache&) = delete;
    file_cache& operator=(const file_cache&) = delete;

    filesize_t get_file_size(filesize_t file_id);
    void write(filesize_t file_id, filesize_t offset, uint8_t data);
    uint8_t read(filesize_t file_id, filesize_t offset);

    file_iterator get_iterator(filesize_t file_id, filesize_t offset = 0);
    file_iterator get_iterator(const far_offset_ptr& ptr);
};
