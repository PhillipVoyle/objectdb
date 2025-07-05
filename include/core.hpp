#pragma once

#include <exception>
#include <stdexcept>
#include <cstdint>
#include <span>
#include <vector>

using filesize_t = uint64_t;
using blob_t = std::vector<uint8_t>;
using span_t = std::span<uint8_t>;

const filesize_t block_size = 4096;
const filesize_t block_file_size = 1024 * 1024 * 10; // 10MB

class object_db_exception : public std::runtime_error
{
public:
    object_db_exception(const std::string& message) : std::runtime_error(message)
    {
    }
};

int compare_span(const std::span<uint8_t>& a, const std::span<uint8_t>& b);
