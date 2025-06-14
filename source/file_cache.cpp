#include <fstream>
#include <filesystem>
#include <string>

#include "../include/file_iterator.hpp"
#include "../include/file_cache.hpp"

static std::string get_filename(const std::filesystem::path& cache_path, filesize_t file_id)
{
    return (cache_path / ("file_" + std::to_string(file_id) + ".bin")).string();
}

filesize_t file_cache::get_file_size(filesize_t file_id)
{
    std::ifstream file(get_filename(cache_path, file_id), std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return 0; // File does not exist or cannot be opened
    }
    filesize_t size = file.tellg();
    file.close();
    return size;
}

void file_cache::write(filesize_t file_id, filesize_t offset, uint8_t data)
{
    std::ofstream file(get_filename(cache_path, file_id), std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        file.open("file_" + std::to_string(file_id) + ".bin", std::ios::binary | std::ios::trunc);
    }
    file.seekp(offset);
    file.put(data);
    file.close();
}

uint8_t file_cache::read(filesize_t file_id, filesize_t offset)
{
    std::ifstream file(get_filename(cache_path, file_id), std::ios::binary);
    if (!file.is_open()) {
        return 0; // File does not exist or cannot be opened
    }
    file.seekg(offset);
    uint8_t data = file.get();
    file.close();
    return data;
}

file_iterator file_cache::get_iterator(filesize_t file_id, filesize_t offset)
{
    return file_iterator(this, file_id, offset);
}