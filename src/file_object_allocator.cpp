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

bool file_object_allocator_impl::write_data(filesize_t offset, const std::span<uint8_t>& data)
{
    if (data.size() != block_size_)
    {
        return false;
    }

    if (offset % block_size_ != 0)
    {
        return false;
    }

    return file_.write_data(offset, data);
}

bool file_object_allocator_impl::read_data(filesize_t offset, const std::span<uint8_t>& data)
{
    if (data.size() != block_size_)
    {
        return false;
    }

    if (offset % block_size_ != 0)
    {
        return false;
    }

    return file_.read_data(offset, data);
}

bool file_object_allocator_impl::allocate_block(filesize_t& offset)
{
    std::vector<uint8_t> first_block(block_size_, 0);
    if (!read_data(0, first_block))
    {
        return false;
    }

    filesize_t freelist_first_block_offset = 0;
    filesize_t freelist_last_block_offset = 0;

    assert(block_size_ >= (sizeof(filesize_t) * 4));

    auto itStart = first_block.begin();
    auto itEnd = itStart + sizeof(filesize_t);
    if (!read_filesize({ itStart, itEnd }, freelist_first_block_offset))
    {
        return false;
    }
    itStart = itEnd;
    itEnd = itStart + sizeof(filesize_t);

    if (!read_filesize({ itStart, itEnd }, freelist_last_block_offset))
    {
        return false;
    }

    if (freelist_first_block_offset != 0)
    {
        std::vector<uint8_t> free_block(block_size_, 0);
        if (!read_data(freelist_first_block_offset, free_block))
        {
            return false;
        }

        filesize_t new_freelist_first_block_offset = 0;
        if (!read_filesize({ free_block.begin(), free_block.begin() + sizeof(filesize_t) }, new_freelist_first_block_offset))
        {
            return false;
        }

        if (!write_filesize({ first_block.begin(), first_block.begin() + sizeof(filesize_t) }, new_freelist_first_block_offset))
        {
            return false;
        }

        // if this is the last node, clear the list
        if (new_freelist_first_block_offset == 0)
        {
            if (!write_filesize({ first_block.begin() + sizeof(filesize_t), first_block.begin() + 2 * sizeof(filesize_t) }, 0))
            {
                return false;
            }
        }

        if (!write_data(0, first_block))
        {
            return false;
        }

        if (!write_data(freelist_first_block_offset, free_block))
        {
            return false;
        }
        offset = freelist_first_block_offset;
        return true;
    }

    return allocate_new_block(offset);
}

bool file_object_allocator_impl::free_block(filesize_t offset)
{
    std::vector<uint8_t> first_block(block_size_, 0);

    // clear the block
    if (!write_data(offset, first_block))
    {
        return false;
    }

    if (!read_data(0, first_block))
    {
        return false;
    }

    filesize_t freelist_first_block_offset = 0;
    filesize_t freelist_last_block_offset = 0;

    assert(block_size_ >= (sizeof(filesize_t) * 4));


    auto itStart = first_block.begin();
    auto itEnd = itStart + sizeof(filesize_t);
    if (!read_filesize({ itStart, itEnd }, freelist_first_block_offset))
    {
        return false;
    }
    itStart = itEnd;
    itEnd = itStart + sizeof(filesize_t);

    if (!read_filesize({ itStart, itEnd }, freelist_last_block_offset))
    {
        return false;
    }

    if (freelist_last_block_offset != 0)
    {
        std::vector<uint8_t> last_block(block_size_, 0);
        if (!write_filesize({ last_block.begin(), last_block.begin() + sizeof(filesize_t) }, offset))
        {
            return false;
        }

        if (!write_filesize({ first_block.begin() + sizeof(filesize_t), first_block.begin() + 2 * sizeof(filesize_t) }, offset))
        {
            return false;
        }

        if (!write_data(freelist_last_block_offset, last_block))
        {
            return false;
        }
    }
    else
    {
        if (!write_filesize({ first_block.begin(), first_block.begin() + sizeof(filesize_t) }, offset))
        {
            return false;
        }

        if (!write_filesize({ first_block.begin() + sizeof(filesize_t), first_block.begin() + 2 * sizeof(filesize_t) }, offset))
        {
            return false;
        }
    }

    if (!write_data(0, first_block))
    {
        return false;
    }

    return true;
}

bool file_object_allocator_impl::allocate_new_block(filesize_t& offset)
{
    offset = file_.get_file_size();
    std::vector<uint8_t> block(block_size_, 0);
    return file_.write_data(offset, block);
}

