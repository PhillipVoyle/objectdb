#pragma once

#include "../include/core.hpp"

class file_cache;

class file_iterator
{
    file_cache* file_cache_;
    filesize_t file_id_ = 0;
    filesize_t offset_ = 0;
public:
    file_iterator(file_cache* cache = nullptr, filesize_t file_id = 0, filesize_t offset = 0);
    uint8_t read();
    void read_bytes(std::span<uint8_t>& span);
    void write(uint8_t data);
    void write_bytes(std::span<const uint8_t> span);
    bool has_next() const;
};


