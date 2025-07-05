#pragma once

#include <cstdint>
#include <vector>
#include <map>
#include <memory>

#include "../include/random_access_file.hpp"
#include "../include/std_random_access_file.hpp"

/// <summary>
/// a reference that knows how to load a kind of thing
/// </summary>
template<typename record_type>
class offset_ptr
{
    filesize_t offset_ = 0;
public:
    offset_ptr() = default;

    explicit(false) offset_ptr(filesize_t offset) : offset_(offset)
    {
    }

    explicit offset_ptr(const std::span<uint8_t>& offset)
    {
        read_filesize(offset, offset_);
    }

    filesize_t get_size() const
    {
        return sizeof(filesize_t);
    }

    filesize_t get_offset() const
    {
        return offset_;
    }

    bool empty() const
    {
        return offset_ == 0;
    }

    bool operator!() const
    {
        return empty();
    }

    void read_from_span(const std::span<uint8_t>& s)
    {
        read_filesize(s, offset_);
    }

    void write_to_span(const std::span<uint8_t>& s)
    {
        write_filesize(s, offset_);
    }

    // I'm torn about having to write both file and span versions of these
    void read_object(random_access_file& raf, record_type& record)
    {
        record.read(offset_, raf);
    }

    void write_object(random_access_file& raf, record_type& record)
    {
        record.write(offset_, raf);
    }
};

class file_ref_cache
{
public:
    virtual ~file_ref_cache() = default;
    virtual std::shared_ptr<random_access_file> acquire_file(uint64_t filename) = 0;
    virtual void release_file(uint64_t filename) = 0;
};

template<typename record_type>
class far_offset_ptr
{
    filesize_t filename_ = 0;
    filesize_t offset_ = 0;
public:
    far_offset_ptr() = default;

    explicit(false) far_offset_ptr(filesize_t filename, filesize_t offset) : filename_(filename), offset_(offset)
    {
    }

    explicit far_offset_ptr(const std::span<uint8_t>& s)
    {
        read_from_span(s);
    }

    filesize_t get_size() const
    {
        return sizeof(filesize_t) * 2;
    }

    filesize_t get_filename()
    {
        return filename_;
    }

    filesize_t get_offset()
    {
        return offset_;
    }

    bool empty() const
    {
        return offset_ == 0 && filename_ == 0;
    }

    bool operator!() const
    {
        return empty();
    }

    void read_from_span(const std::span<uint8_t>& s)
    {
        filename_ = 0;
        offset_ = 0;

        span_t filename_span(s.begin(), s.begin() + sizeof(filesize_t));
        span_t offset_span(s.begin() + sizeof(filesize_t), s.begin() + sizeof(filesize_t) * 2);

        read_filesize(filename_span, filename_);
        read_filesize(offset_span, offset_);
    }

    void write_to_span(const std::span<uint8_t>& s) const
    {
        if (s.size() < get_size())
        {
            throw object_db_exception("span size is less than required size for far_offset_ptr");
        }
        span_t filename_span(s.begin(), s.begin() + sizeof(filesize_t));
        span_t offset_span(s.begin() + sizeof(filesize_t), s.begin() + sizeof(filesize_t) * 2);
        write_filesize(filename_span, filename_);
        write_filesize(offset_span, offset_);
    }

    void read_object(file_ref_cache& ref_cache, record_type& record)
    {
        blob_t data(record.get_size(), 0);
        auto file = ref_cache.acquire_file(filename_);
        file->read_data(offset_, data);
        record.read_from_span({ data.data(), data.data() + data.size() });
    }

    void write_object(file_ref_cache& ref_cache, record_type& record)
    {
        auto file = ref_cache.acquire_file(filename_);
        blob_t data(record.get_size(), 0);
        record.write_to_span({ data.data(), data.data() + data.size() });
        file->write_data(offset_, data);
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

    void read_from_file(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);
        raf.read_data(offset, block);

        auto it = block.begin();
        auto it2 = it + next_ptr_.get_size();
        next_ptr_ = offset_ptr<free_list_node>({ it, it2 });
    }

    void write_to_file(filesize_t offset, random_access_file& raf) const
    {
        std::vector<uint8_t> block(block_size_, 0);
        auto it = block.begin();
        auto it2 = it + next_ptr_.get_size();
        write_filesize({ it, it2 }, next_ptr_.get_offset());
        raf.write_data(offset, block);
    }
};

class null_space
{
public:
    explicit null_space([[maybe_unused]] filesize_t block_size, [[maybe_unused]] filesize_t remaining_space)
    {
    }

    filesize_t get_size() const
    {
        return 0; // null space has no size
    }

    void read_from_span([[maybe_unused]] const std::span<uint8_t>& s) const
    {
        // no-op for null space
    }

    void write_to_span([[maybe_unused]] const std::span<uint8_t>& s) const
    {
        // no-op for null space
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
    {
        return remaining_space_;
    }

    void set_freelist_start_offset(offset_ptr<free_list_node> start)
    {
        freelist_start_offset_ = start;
    }

    void set_freelist_end_offset(offset_ptr<free_list_node> end)
    {
        freelist_end_offset_ = end;
    }

    void read_object(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);
        raf.read_data(offset, block);

        auto it = block.begin();
        auto it2 = it + freelist_start_offset_.get_size();
        freelist_start_offset_.read_from_span({ it, it2 });
        auto it3 = it2 + freelist_end_offset_.get_size();
        freelist_end_offset_.read_from_span({ it2, it3 });
        auto it4 = it3 + remaining_space_.get_size();
        remaining_space_.read_from_span({ it3, it4 });
    }

    void write_object(filesize_t offset, random_access_file& raf)
    {
        std::vector<uint8_t> block(block_size_, 0);

        auto it = block.begin();
        auto it2 = it + freelist_start_offset_.get_size();
        freelist_start_offset_.write_to_span({ it, it2 });
        auto it3 = it2 + freelist_end_offset_.get_size();
        freelist_end_offset_.write_to_span({ it2, it3 });
        auto it4 = it3 + remaining_space_.get_size();
        remaining_space_.write_to_span({ it3, it4 });
        raf.write_data(offset, block);
    }
};
