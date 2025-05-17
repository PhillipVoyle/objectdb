#pragma once

#include <exception>
#include <stdexcept>
#include <cstdint>
using filesize_t = uint64_t;

class object_db_exception : public std::runtime_error
{
public:
    object_db_exception(const std::string& message) : std::runtime_error(message)
    {
    }
};

