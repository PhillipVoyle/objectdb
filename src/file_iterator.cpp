
#include "../include/file_iterator.hpp"
#include "../include/file_cache.hpp"

file_iterator::file_iterator(file_cache* cache, filesize_t file_id, filesize_t offset) : file_cache_(cache), file_id_(file_id), offset_(offset)
{
    if (!file_cache_) {
        throw object_db_exception("File cache is not initialized.");
    }
}
uint8_t file_iterator::read()  
{
    if (!file_cache_) {
        throw object_db_exception("File cache is not initialized.");  
    } 

    auto offset = offset_++;
    uint8_t data = file_cache_->read(file_id_, offset);
    return data;
}

void file_iterator::read_bytes(std::span<uint8_t>& bytes)
{
    auto offset = offset_;
    offset_ += bytes.size();
    file_cache_->read_bytes(file_id_, offset, bytes);
}


bool file_iterator::has_next() const
{
    if (!file_cache_) {
        throw object_db_exception("File cache is not initialized.");
    }
    return file_cache_->get_file_size(file_id_) > offset_;
}

void file_iterator::write(uint8_t data)
{
    auto offset = offset_++;
    file_cache_->write(file_id_, offset, data);
}

void file_iterator::write_bytes(std::span<const uint8_t> bytes)
{
    auto offset = offset_;
    offset_ += bytes.size();
    file_cache_->write_bytes(file_id_, offset, bytes);
}