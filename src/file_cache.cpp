#include <fstream>
#include <filesystem>
#include <string>

#include "../include/file_iterator.hpp"
#include "../include/file_cache.hpp"

#include <list>

// Helper function to close and erase the least recently used file
void file_cache::evict_if_needed()
{
    if (file_streams.size() > 4) {
        // Remove any file_ids from lru_list that are not in file_streams
        for (auto it = lru_list.begin(); it != lru_list.end();) {
            if (file_streams.find(*it) == file_streams.end())
                it = lru_list.erase(it);
            else
                ++it;
        }
        // If still over limit, evict the least recently used
        while (file_streams.size() > 4 && !lru_list.empty()) {
            filesize_t lru_id = lru_list.front();
            lru_list.pop_front();
            file_streams[lru_id].close();
            file_streams.erase(lru_id);
        }
    }
}

std::fstream& file_cache::get_stream(filesize_t file_id, std::ios::openmode mode)
{
    static std::list<filesize_t> lru_list;
    auto it = file_streams.find(file_id);
    if (it == file_streams.end()) {
        // Open new file stream
        std::string filename = get_filename(cache_path, file_id);
        if (!std::filesystem::exists(cache_path))
        {
            std::filesystem::create_directories(cache_path);
        }
        std::fstream fs(filename, mode);
        if (!fs.is_open())
        {
            if (mode & std::ios::out)
            {
                // Try to create the file if it doesn't exist
                fs.open(filename, std::ios::binary | std::ios::trunc | std::ios::out);
                fs.close();
                fs.open(filename, std::ios::binary | std::ios::in | std::ios::out);
            }
        }
        else
        {
            // We didn't accidentally create it. open file for writing instead
            fs.close();
            fs.open(filename, std::ios::binary | std::ios::in | std::ios::out);
        }

        file_streams[file_id] = std::move(fs);
        lru_list.push_back(file_id);
        evict_if_needed();
        return file_streams[file_id];
    } else {
        // Move to back (most recently used)
        lru_list.remove(file_id);
        lru_list.push_back(file_id);
        return it->second;
    }
}

filesize_t file_cache::get_file_size(filesize_t file_id)
{
    std::fstream& file = get_stream(file_id, std::ios::binary | std::ios::in);
    if (!file.is_open()) {
        file_streams.erase(file_id);
        return 0;
    }
    file.seekg(0, std::ios::end);
    filesize_t size = file.tellg();
    file.seekg(0, std::ios::beg);
    return size;
}

void file_cache::write(filesize_t file_id, filesize_t offset, uint8_t data)
{
    std::fstream& file = get_stream(file_id, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        file_streams.erase(file_id);
        throw object_db_exception("could not open file for writing");
    }
    file.seekp(offset);
    file.put(data);
    file.flush();
}

uint8_t file_cache::read(filesize_t file_id, filesize_t offset)
{
    std::fstream& file = get_stream(file_id, std::ios::binary | std::ios::in);
    if (!file.is_open()) {
        file_streams.erase(file_id);
        return 0;
    }
    file.seekg(offset);
    //int value = file.get();
    uint8_t value = 0;
    file.read((char*) &value, 1);
    if (value == EOF) return 0;
    return static_cast<uint8_t>(value);
}

std::string file_cache::get_filename(const std::filesystem::path& cache_path, filesize_t file_id)
{
    return (cache_path / ("file_" + std::to_string(file_id) + ".bin")).string();
}

file_iterator file_cache::get_iterator(filesize_t file_id, filesize_t offset)
{
    return file_iterator(this, file_id, offset);
}
