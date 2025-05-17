#include "../include/file_object_allocator.hpp"
#include <exception>
#include <vector>
#include <cassert>

file_object_allocator_impl::file_object_allocator_impl(random_access_file& file, size_t block_size)
    : file_(file), block_size_(block_size)
{
    auto file_size = file_.get_file_size();
    if (file_size == 0)
    {
        std::vector<uint8_t> block(block_size_, 0);
        file_.write_data(0, block);
    }
}

filesize_t file_object_allocator_impl::get_block_size() const
{
    return block_size_;
}

filesize_t file_object_allocator_impl::get_file_size()
{
    return file_.get_file_size();
}

void file_object_allocator_impl::write_data(filesize_t offset, const std::span<uint8_t>& data)
{
    if (data.size() != block_size_)
    {
        throw object_db_exception("expected data size to equal block size");
    }

    if (offset % block_size_ != 0)
    {
        throw object_db_exception("offset must be a multiple of block size");
    }

    file_.write_data(offset, data);
}

void file_object_allocator_impl::read_data(filesize_t offset, const std::span<uint8_t>& data)
{
    if (data.size() != block_size_)
    {
        throw object_db_exception("expected data size to equal block size");
    }

    if (offset % block_size_ != 0)
    {
        throw object_db_exception("offset must be a multiple of block size");
    }

    file_.read_data(offset, data);
}

void file_object_allocator_impl::allocate_block(filesize_t& offset)
{
    std::vector<uint8_t> first_block(block_size_, 0);
    read_data(0, first_block);

    filesize_t freelist_first_block_offset = 0;
    filesize_t freelist_last_block_offset = 0;

    assert(block_size_ >= (sizeof(filesize_t) * 4));

    auto itStart = first_block.begin();
    auto itEnd = itStart + sizeof(filesize_t);
    read_filesize({ itStart, itEnd }, freelist_first_block_offset);
    itStart = itEnd;
    itEnd = itStart + sizeof(filesize_t);

    read_filesize({ itStart, itEnd }, freelist_last_block_offset);

    if (freelist_first_block_offset != 0)
    {
        // use the first block in the free list
        std::vector<uint8_t> free_block(block_size_, 0);
        read_data(freelist_first_block_offset, free_block);

        filesize_t new_freelist_first_block_offset = 0;
        read_filesize({ free_block.begin(), free_block.begin() + sizeof(filesize_t) }, new_freelist_first_block_offset);
        write_filesize({ first_block.begin(), first_block.begin() + sizeof(filesize_t) }, new_freelist_first_block_offset);

        // if this is the last node, clear the list
        if (new_freelist_first_block_offset == 0)
        {
            write_filesize({ first_block.begin() + sizeof(filesize_t), first_block.begin() + 2 * sizeof(filesize_t) }, 0);
        }

        write_data(0, first_block);
        write_data(freelist_first_block_offset, free_block);
        offset = freelist_first_block_offset;
    }
    else
    {
        // no free blocks, allocate a new one
        allocate_new_block(offset);
    }
}

void file_object_allocator_impl::free_block(filesize_t offset)
{
    std::vector<uint8_t> first_block(block_size_, 0);

    // clear the block
    write_data(offset, first_block);
    read_data(0, first_block);

    filesize_t freelist_first_block_offset = 0;
    filesize_t freelist_last_block_offset = 0;

    assert(block_size_ >= (sizeof(filesize_t) * 4));


    auto itStart = first_block.begin();
    auto itEnd = itStart + sizeof(filesize_t);
    read_filesize({ itStart, itEnd }, freelist_first_block_offset);
    itStart = itEnd;
    itEnd = itStart + sizeof(filesize_t);

    read_filesize({ itStart, itEnd }, freelist_last_block_offset);

    if (freelist_last_block_offset != 0)
    {
        std::vector<uint8_t> last_block(block_size_, 0);
        write_filesize({ last_block.begin(), last_block.begin() + sizeof(filesize_t) }, offset);
        write_filesize({ first_block.begin() + sizeof(filesize_t), first_block.begin() + 2 * sizeof(filesize_t) }, offset);
        write_data(freelist_last_block_offset, last_block);
    }
    else
    {
        write_filesize({ first_block.begin(), first_block.begin() + sizeof(filesize_t) }, offset);
        write_filesize({ first_block.begin() + sizeof(filesize_t), first_block.begin() + 2 * sizeof(filesize_t) }, offset);
    }

    write_data(0, first_block);
}

void file_object_allocator_impl::allocate_new_block(filesize_t& offset)
{
    offset = file_.get_file_size();
    std::vector<uint8_t> block(block_size_, 0);
    file_.write_data(offset, block);
}

