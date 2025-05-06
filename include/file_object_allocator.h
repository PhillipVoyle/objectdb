#pragma once
#include <string>
#include <span>
#include <cstdint>

#include "random_access_file.hpp"

class file_object_allocator : public random_access_file
{
public:
   virtual ~file_object_allocator() = default;

   virtual bool allocate_block(filesize_t& offset) = 0;
   virtual bool free_block(filesize_t offset) = 0;
   virtual filesize_t get_block_size() const = 0;
};

class file_object_allocator_impl
    : public file_object_allocator
{
private:
    random_access_file& file_;
    size_t block_size_;
    bool allocate_new_block(filesize_t& offset);
public:
    file_object_allocator_impl(random_access_file& file, size_t block_size);
    ~file_object_allocator_impl() override;

    filesize_t get_block_size() const override;
    bool write_data(filesize_t blocknum, const std::span<uint8_t>& data) override;
    bool read_data(filesize_t blocknum, const std::span<uint8_t>& data) override;
    bool allocate_block(filesize_t& offset) override;
    bool free_block(filesize_t offset) override;
};
