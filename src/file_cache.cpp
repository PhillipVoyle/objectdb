#include <fstream>
#include <filesystem>
#include <string>

#include "../include/file_iterator.hpp"
#include "../include/file_cache.hpp"
#include "../include/far_offset_ptr.hpp"

#include <list>

block_cache::block_cache(int lru_size) :lru_max_(lru_size)
{
}

std::shared_ptr<std::vector<uint8_t>> block_cache::get_block(filesize_t filename, filesize_t offset)
{
    auto tup = std::tuple(filename, offset);
    auto it = blocks_.find(tup);

    std::shared_ptr<std::vector<uint8_t>> result;

    if (it == blocks_.end())
    {
        block_cache_entry entry{ };
        entry.block = std::make_shared<std::vector<uint8_t>>();

        lru_block_list_.push_back(tup);
        entry.lru_iterator = std::prev(lru_block_list_.end());

        blocks_.try_emplace(tup, entry);
        result = entry.block;

        // pop from cache
        while (lru_block_list_.size() > lru_max_)
        {
            auto front_tup = lru_block_list_.front();
            lru_block_list_.pop_front();

            blocks_.erase(front_tup);
        }
    }
    else
    {
        auto& entry = it->second;
        lru_block_list_.erase(entry.lru_iterator);

        lru_block_list_.push_back(tup);
        entry.lru_iterator = std::prev(lru_block_list_.end());
        result = entry.block;
    }
    return result;
}

bool block_cache::exists(filesize_t filename, filesize_t offset)
{
    return blocks_.find(std::tuple(filename, offset)) != blocks_.end();
}

// Helper function to close and erase the least recently used file
void concrete_file_cache::evict_file_if_needed()
{
    if (file_streams.size() > 4) {
        // Remove any file_ids from lru_file_list that are not in file_streams
        for (auto it = lru_file_list.begin(); it != lru_file_list.end();) {
            if (file_streams.find(*it) == file_streams.end())
                it = lru_file_list.erase(it);
            else
                ++it;
        }
        // If still over limit, evict the least recently used
        while (file_streams.size() > 4 && !lru_file_list.empty()) {
            filesize_t lru_id = lru_file_list.front();
            lru_file_list.pop_front();
            file_streams[lru_id].close();
            file_streams.erase(lru_id);
        }
    }
}

std::fstream& concrete_file_cache::get_stream(filesize_t file_id, std::ios::openmode mode)
{
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
        lru_file_list.push_back(file_id);
        evict_file_if_needed();
        return file_streams[file_id];
    } else {
        // Move to back (most recently used)
        lru_file_list.remove(file_id);
        lru_file_list.push_back(file_id);
        return it->second;
    }
}

filesize_t concrete_file_cache::get_file_size(filesize_t file_id)
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

void concrete_file_cache::write(filesize_t file_id, filesize_t offset, uint8_t data)
{
    auto block_offset_remainder = offset % 4096;
    auto block_offset_base = offset - block_offset_remainder;

    std::fstream& file = get_stream(file_id, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        file_streams.erase(file_id);
        throw object_db_exception("could not open file for writing");
    }
    file.seekp(offset);
    file.put(data);

    if (blocks_.exists(file_id, block_offset_base))
    {
        auto block = blocks_.get_block(file_id, block_offset_base);
        if (block)
        {
            block->at(block_offset_remainder) = data;
        }
    }

    file.flush();
}

uint8_t concrete_file_cache::read(filesize_t file_id, filesize_t offset)
{
    auto block_offset_remainder = offset % 4096;
    auto block_offset_base = offset - block_offset_remainder;

    auto block = blocks_.get_block(file_id, block_offset_base);
    if (block)
    {
        if (block->size() == 0)
        {
            std::fstream& file = get_stream(file_id, std::ios::binary | std::ios::in);
            if (!file.is_open()) {
                file_streams.erase(file_id);
                return 0;
            }
            file.seekg(offset);

            block->resize(4096);
            file.read((char*)&(block->at(0)), 4096);
            if (file.fail())
            {
                block->resize(0);
                return 0;
            }
        }

        return block->at(block_offset_remainder);
    }
    else
    {
        throw object_db_exception("Block cache failure");
    }
}

void concrete_file_cache::write_bytes(filesize_t file_id, filesize_t offset, std::span<const uint8_t> data)
{
    auto block_offset_remainder = offset % 4096;
    auto block_offset_base = offset - block_offset_remainder;

    if (block_offset_remainder == 0 && data.size() == 4096)
    {
        auto block = blocks_.get_block(file_id, block_offset_base);
        std::fstream& file = get_stream(file_id, std::ios::binary | std::ios::in | std::ios::out);
        file.seekg(offset);

        block->resize(4096);
        file.write((char*)&(block->at(0)), 4096);
        for (int a = 0; a < 4096; a++)
        {
            block->at(a) = data[a];
        }
    }
    else
    {
        auto current_offset = offset;
        for (auto d : data)
        {
            write(file_id, current_offset, d);
            current_offset++;
        }
    }
}

//todo: see if we can improve the performance here
void concrete_file_cache::read_bytes(filesize_t file_id, filesize_t offset, std::span<uint8_t> data)
{
    auto current_offset = offset;
    for (auto& d : data)
    {
        d = read(file_id, current_offset);
        current_offset++;
    }
}

std::string concrete_file_cache::get_filename(const std::filesystem::path& cache_path, filesize_t file_id)
{
    return (cache_path / ("file_" + std::to_string(file_id) + ".bin")).string();
}

file_iterator concrete_file_cache::get_iterator(const far_offset_ptr& ptr)
{
    return file_iterator(this, ptr.get_file_id(), ptr.get_offset());
}

file_iterator concrete_file_cache::get_iterator(filesize_t file_id, filesize_t offset)
{
    return file_iterator(this, file_id, offset);
}
