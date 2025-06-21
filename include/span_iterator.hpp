#pragma once

#include <span>
#include <cstdint>

class span_iterator
{
    std::span<uint8_t> data_;
    std::size_t offset_ = 0;
public:
    span_iterator(std::span<uint8_t> data, std::size_t offset = 0);
    uint8_t read();
    void write(uint8_t value);
    bool has_next() const;
};
