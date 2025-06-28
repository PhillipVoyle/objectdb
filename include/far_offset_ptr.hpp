#pragma once

#include "../include/core.hpp"
#include "../include/binary_iterator.hpp"

class far_offset_ptr
{
private:
    filesize_t file_id;
    filesize_t offset;
public:
    // Default constructor
    far_offset_ptr() : file_id(0), offset(0) {}

    // Parameterized constructor
    far_offset_ptr(filesize_t file_id, filesize_t offset)
        : file_id(file_id), offset(offset) {}

    // Copy constructor
    far_offset_ptr(const far_offset_ptr& other)
        : file_id(other.file_id), offset(other.offset) {}

    // Move constructor
    far_offset_ptr(far_offset_ptr&& other) noexcept
        : file_id(other.file_id), offset(other.offset)
    {
        other.file_id = 0;
        other.offset = 0;
    }

    bool operator==(const far_offset_ptr& other) const = default;

    // Copy assignment
    far_offset_ptr& operator=(const far_offset_ptr& other)
    {
        if (this != &other)
        {
            file_id = other.file_id;
            offset = other.offset;
        }
        return *this;
    }

    // Move assignment
    far_offset_ptr& operator=(far_offset_ptr&& other) noexcept
    {
        if (this != &other)
        {
            file_id = other.file_id;
            offset = other.offset;
            other.file_id = 0;
            other.offset = 0;
        }
        return *this;
    }

    static constexpr filesize_t get_size()
    {
        return sizeof(filesize_t) * 2;
    }

    // Destructor
    ~far_offset_ptr() = default;

    // Accessors
    filesize_t get_file_id() const { return file_id; }
    filesize_t get_offset() const { return offset; }

    // Setters
    void set_file_id(filesize_t new_file_id) { file_id = new_file_id; }
    void set_offset(filesize_t new_offset) { offset = new_offset; }

    // Read from binary iterator
    template<Binary_iterator It>
    void read(It& it)
    {
        file_id = read_filesize(it);
        offset = read_filesize(it);
    }

    // Write to binary iterator
    template<Binary_iterator It>
    void write(It& it) const
    {
        write_filesize(it, file_id);
        write_filesize(it, offset);
    }
};
