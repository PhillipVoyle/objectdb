#pragma once

#include <span>
#include <cstdint>

using filesize_t = uint64_t;

// todo: consider converting to concept
class random_access_file
{
public:
    virtual ~random_access_file() {}
    virtual filesize_t get_file_size() = 0;
    virtual bool write_data(filesize_t offset, const std::span<uint8_t>& data) = 0;
    virtual bool read_data(filesize_t offset, std::span<uint8_t>& data) = 0;
};


bool read_uint32(random_access_file& f, filesize_t offset, uint32_t& data);
bool read_uint64(random_access_file& f, filesize_t offset, uint64_t& data);
bool read_uint8(random_access_file& f, filesize_t offset, uint8_t& data);
bool read_int32(random_access_file& f, filesize_t offset, int32_t& data);
bool read_int64(random_access_file& f, filesize_t offset, int64_t& data);
bool read_int8(random_access_file& f, filesize_t offset, int8_t& data);
bool read_char(random_access_file& f, filesize_t offset, char& data);

bool write_uint32(random_access_file& f, filesize_t offset, uint32_t data);
bool write_uint64(random_access_file& f, filesize_t offset, uint64_t data);
bool write_uint8(random_access_file& f, filesize_t offset, uint8_t data);
bool write_int32(random_access_file& f, filesize_t offset, int32_t data);
bool write_int64(random_access_file& f, filesize_t offset, int64_t data);
bool write_int8(random_access_file& f, filesize_t offset, int8_t data);
bool write_char(random_access_file& f, filesize_t offset, char data);

inline bool read_filesize(random_access_file& f, filesize_t offset, filesize_t& data)
{
    return read_uint64(f, offset, data);
}

inline bool write_filesize(random_access_file& f, filesize_t offset, filesize_t data)
{
    return write_uint64(f, offset, data);
}
