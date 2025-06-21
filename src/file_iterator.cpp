
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
    return file_cache_->read(file_id_, offset_++);
}
bool file_iterator::has_next() const
{
    if (!file_cache_) {
        throw object_db_exception("File cache is not initialized.");
    }
    return file_cache_->get_file_size(file_id_) > offset_;
}