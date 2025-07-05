#pragma once
#include "../include/core.hpp"
#include <span>
#include <cstdint>
#include <type_traits>
#include <sstream>

template<typename T>
concept Binary_iterator
= requires(T t, uint8_t b) {
    { t.read() } -> std::convertible_to<uint8_t>;
    { t.write(b) } -> std::same_as<void>;
    { t.has_next() } -> std::convertible_to<bool>;
};

// Write a uint8_t to the iterator
template<Binary_iterator It>
void write_uint8(It& it, uint8_t value) {
    it.write(value);
}

// Write a int8_t to the iterator
template<Binary_iterator It>
void write_int8(It& it, int8_t value) {
    it.write(static_cast<uint8_t>(value));
}

// Write a uint16_t to the iterator (big-endian)
template<Binary_iterator It>
void write_uint16(It& it, uint16_t value) {
    it.write(static_cast<uint8_t>((value >> 8) & 0xFF));
    it.write(static_cast<uint8_t>(value & 0xFF));
}

// Write a int16_t to the iterator (big-endian)
template<Binary_iterator It>
void write_int16(It& it, int16_t value) {
    write_uint16(it, static_cast<uint16_t>(value));
}

// Write a uint32_t to the iterator (big-endian)
template<Binary_iterator It>
void write_uint32(It& it, uint32_t value) {
    for (int i = 3; i >= 0; --i) {
        it.write(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
    }
}

// Write a int32_t to the iterator (big-endian)
template<Binary_iterator It>
void write_int32(It& it, int32_t value) {
    write_uint32(it, static_cast<uint32_t>(value));
}

// Write a uint64_t to the iterator (big-endian)
template<Binary_iterator It>
void write_uint64(It& it, uint64_t value) {
    for (int i = 7; i >= 0; --i) {
        it.write(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
    }
}

// Write a int64_t to the iterator (big-endian)
template<Binary_iterator It>
void write_int64(It& it, int64_t value) {
    write_uint64(it, static_cast<uint64_t>(value));
}

template<Binary_iterator It>
void write_string(It& it, const std::string& str)
{
    for (char c : str)
    {
        if (it.has_next())
        {
            it.write(static_cast<uint8_t>(c));
        }
        else
        {
            throw object_db_exception("Binary iterator does not have enough space to write the string.");
        }
    }

    if (it.has_next())
    {
        it.write(0);
    }
}

// Write a span of uint8_t to the iterator
template<Binary_iterator It>
void write_span(It& it, std::span<const uint8_t> data) {
    for (uint8_t b : data) {
        it.write(b);
    }
}

// Read a uint8_t from the iterator
template<Binary_iterator It>
uint8_t read_uint8(It& it) {
    return it.read();
}

// Read a int8_t from the iterator
template<Binary_iterator It>
int8_t read_int8(It& it) {
    return static_cast<int8_t>(it.read());
}

// Read a uint16_t from the iterator (big-endian)
template<Binary_iterator It>
uint16_t read_uint16(It& it) {
    uint16_t b0 = it.read();
    uint16_t b1 = it.read();
    return static_cast<uint16_t>((b0 << 8) | b1);
}

// Read a int16_t from the iterator (big-endian)
template<Binary_iterator It>
int16_t read_int16(It& it) {
    return static_cast<int16_t>(read_uint16(it));
}

// Read a uint32_t from the iterator (big-endian)
template<Binary_iterator It>
uint32_t read_uint32(It& it) {
    uint32_t result = 0;
    for (int i = 3; i >= 0; --i) {
        result |= static_cast<uint32_t>(it.read()) << (8 * i);
    }
    return result;
}

// Read a int32_t from the iterator (big-endian)
template<Binary_iterator It>
int32_t read_int32(It& it) {
    return static_cast<int32_t>(read_uint32(it));
}

// Read a uint64_t from the iterator (big-endian)
template<Binary_iterator It>
uint64_t read_uint64(It& it) {
    uint64_t result = 0;
    for (int i = 7; i >= 0; --i) {
        result |= static_cast<uint64_t>(it.read()) << (8 * i);
    }
    return result;
}

// Read a int64_t from the iterator (big-endian)
template<Binary_iterator It>
int64_t read_int64(It& it) {
    return static_cast<int64_t>(read_uint64(it));
}

// Read a span of uint8_t from the iterator
template<Binary_iterator It>
void read_span(It& it, std::span<uint8_t> data) {
    for (auto& b : data) {
        b = it.read();
    }
}

// Write a filesize_t (uint64_t) to the iterator (big-endian)
template<Binary_iterator It>
void write_filesize(It& it, uint64_t value) {
    write_uint64(it, value);
}

// Read a filesize_t (uint64_t) from the iterator (big-endian)
template<Binary_iterator It>
uint64_t read_filesize(It& it) {
    return read_uint64(it);
}

template<Binary_iterator It>
std::string read_string(It& it)
{
    std::stringstream ss;
    while (it.has_next())
    {
        uint8_t byte = it.read();
        if (byte == 0)
        {
            break; // Null terminator
        }
        ss << static_cast<char>(byte);
    }
    return ss.str();
}
