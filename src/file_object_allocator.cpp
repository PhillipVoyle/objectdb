#include "../include/file_object_allocator.h"
#include <exception>
#include <vector>

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
    return allocate_new_block(offset);
}

bool file_object_allocator_impl::allocate_new_block(filesize_t& offset)
{
    auto result = file_.get_file_size();
    std::vector<uint8_t> block(block_size_, 0);
    return file_.write_data(result, block);
}

