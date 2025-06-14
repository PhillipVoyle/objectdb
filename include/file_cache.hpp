#pragma once  

#include <filesystem>  
#include <map>  
#include <memory>  
#include "../include/core.hpp"  

class file_iterator;  

class file_cache  
{  
    std::filesystem::path cache_path; // Use the alias 'fs::path' to resolve incomplete type error  

public:  
    file_cache(const std::filesystem::path& path) : cache_path(path) {}
    filesize_t get_file_size(filesize_t file_id);  
    void write(filesize_t file_id, filesize_t offset, uint8_t data);  
    uint8_t read(filesize_t file_id, filesize_t offset);  

    file_iterator get_iterator(filesize_t file_id, filesize_t offset = 0);  
};
