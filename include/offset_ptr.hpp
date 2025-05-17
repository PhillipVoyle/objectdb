#pragma once

#include <cstdint>
#include <vector>
#include "../include/random_access_file.hpp"


/// <summary>
/// a reference that knows how to load a kind of thing
/// </summary>
template<typename class record_type>
class offset_ptr
{
    filesize_t offset_ = 0;
public:
    offset_ptr() : offset_(0)
    {
    }

    explicit(false) offset_ptr(filesize_t offset) : offset_(offset)
    {
    }

    offset_ptr(const std::span<uint8_t>& offset)
    {
        offset_ = 0;
        read_filesize(offset, offset_);
    }

    filesize_t get_size() const
    {
        return sizeof(uint8_t);
    }

    filesize_t get_offset()
    {
        return offset_;
    }

    bool empty()
    {
        return offset_ == 0;
    }

    bool read_from_span(const std::span<uint8_t>& s)
    {
        return read_filesize(s, offset_);
    }

    bool write_to_span(const std::span<uint8_t>& s)
    {
        return write_filesize(s, offset_);
    }

    // I'm torn about having to write both file and span versions of these
    bool read_object(random_access_file& raf, record_type& record)
    {
        return record.read(offset_, raf);
    }

    bool write_object(random_access_file& raf, record_type& record const)
    {
        return record.write(offset_, raf);
    }
};

class free_list_node
{
public:
    offset_ptr<free_list_node> next_ptr_;

    filesize_t block_size_;

    explicit free_list_node(filesize_t block_size) : block_size_(block_size)
    {
    }

    bool read_from_file(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);
        if (!raf.read_data(offset, block))
        {
            return false;
        }

        auto it = block.begin();
        auto it2 = it + next_ptr_.get_size();
        next_ptr_ = offset_ptr<free_list_node>({ it, it2 });
        return true;
    }

    bool read_from_file(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);
        auto it = block.begin();
        auto it2 = it + next_ptr_.get_size();
        if (!write_filesize({ it, it2 }, next_ptr_.get_offset()))
        {
            return false;
        }
        return raf.write_data(offset, block);
    }
};

class null_space
{
public:
    explicit null_space(int block_size, int remaining_space)
    {
    }

    bool read_from_span(const std::span<uint8_t>& s) const
    {
        return true;
    }

    bool write_to_span(const std::span<uint8_t>& s) const
    {
        return true;
    }
};


template<typename remaining_space>
class root_node
{
    filesize_t block_size_;
    offset_ptr<free_list_node> freelist_start_offset_;
    offset_ptr<free_list_node> freelist_end_offset_;
    remaining_space remaining_space_;
public:

    explicit root_node(filesize_t block_size) :
        block_size_(block_size),
        remaining_space_(block_size, block_size - freelist_start_offset_.get_size() - freelist_end_offset_.get_size())
    {
    }

    offset_ptr<free_list_node> get_freelist_start_offset()
    {
        return freelist_start_offset_;
    }

    offset_ptr<free_list_node> get_freelist_end_offset()
    {
        return freelist_end_offset_;
    }

    remaining_space& get_remaining_space()

    void set_freelist_start_offset(offset_ptr<free_list_node> start)
    {
        freelist_start_offset = start;
    }

    void set_freelist_end_offset(offset_ptr<free_list_node> end)
    {
        freelist_end_offset = end;
    }

    bool read_object(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);
        if (!raf.read_data(offset, block))
        {
            return false;
        }

        auto it = block.begin();
        auto it2 = it + freelist_start_offset_.get_size();
        if (!freelist_start_offset_.read_from_span({ it, it2 }))
        {
            return false;
        }
        auto it3 = it2 + freelist_end_offset_.get_size();
        if (!freelist_end_offset_.read_from_span({ it2, it3 }))
        {
            return false;
        }
        auto it4 = it3 + remaining_space.get_size();
        return remaining_space.read_from_span({ it3, it4 });
    }

    bool write_object(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);

        auto it = block.begin();
        auto it2 = it + freelist_start_offset_.get_size();
        if (!freelist_start_offset_.write_to_span({ it, it2 }))
        {
            return false;
        }
        auto it3 = it2 + freelist_end_offset_.get_size();
        if (!freelist_end_offset_.write_to_span({ it2, it3 }))
        {
            return false;
        }
        auto it4 = it3 + remaining_space.get_size();
        if (!remaining_space.write_to_span({ it3, it4 }))
        {
            return false;
        }

        return raf.write_data(offset, block);
    }
};
