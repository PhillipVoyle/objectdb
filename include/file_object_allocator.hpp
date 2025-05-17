#pragma once
#include <string>
#include <span>
#include <cstdint>

#include "random_access_file.hpp"

class file_object_allocator : public random_access_file
{
public:
   ~file_object_allocator() override = default;

   virtual void allocate_block(filesize_t& offset) = 0;
   virtual void free_block(filesize_t offset) = 0;
   virtual filesize_t get_block_size() const = 0;
};

class file_object_allocator_impl
    : public file_object_allocator
{
private:
    random_access_file& file_;
    size_t block_size_;
    void allocate_new_block(filesize_t& offset);
public:
    file_object_allocator_impl(random_access_file& file, size_t block_size);
    ~file_object_allocator_impl() final = default;

    filesize_t get_block_size() const override;
    filesize_t get_file_size() override;
    void write_data(filesize_t blocknum, const std::span<uint8_t>& data) override;
    void read_data(filesize_t blocknum, const std::span<uint8_t>& data) override;
    void allocate_block(filesize_t& offset) override;
    void free_block(filesize_t offset) override;
};
