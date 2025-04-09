#include "../include/random_access_file.hpp"
#include <vector>
#include <span>
#include <cassert>

bool read_uint32(random_access_file& f, filesize_t offset, uint32_t& data)
{
    std::vector<uint8_t> vec(4);
    std::span<uint8_t> s(vec);
    assert(s.size() == 4);
    assert(s.size_bytes() == 4);
    if (!f.read_data(offset, s))
    {
        return false;
    }

    uint32_t result = 0;
    auto it = vec.begin();

    result = *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it;

    data = result;
    return true;
}

bool read_uint64(random_access_file& f, filesize_t offset, uint64_t& data)
{
    std::vector<uint8_t> vec(8);
    std::span<uint8_t> s(vec);
    assert(s.size() == 8);
    assert(s.size_bytes() == 8);
    if (!f.read_data(offset, s))
    {
        return false;
    }

    uint64_t result = 0;
    auto it = vec.begin();

    result = *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;

    result = *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it;

    data = result;
    return true;
}

bool read_uint8(random_access_file& f, filesize_t offset, uint8_t& data)
{
    std::vector<uint8_t> vec(1);
    std::span<uint8_t> s(vec);
    assert(s.size() == 1);
    assert(s.size_bytes() == 1);
    if (!f.read_data(offset, s))
    {
        return false;
    }

    uint64_t result = 0;
    auto it = vec.begin();

    result = *it;

    data = result;
    return true;
}

bool read_int32(random_access_file& f, filesize_t offset, int32_t& data)
{
    uint32_t stg = 0;
    if (read_uint32(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool read_int64(random_access_file& f, filesize_t offset, int64_t& data)
{
    uint64_t stg = 0;
    if (read_uint64(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool read_int8(random_access_file& f, filesize_t offset, int8_t& data)
{
    uint8_t stg = 0;
    if (read_uint8(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool read_char(random_access_file& f, filesize_t offset, char& data)
{
    uint8_t stg = 0;
    if (read_uint8(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool write_uint32(random_access_file& f, filesize_t offset, uint32_t data)
{
    std::vector<uint8_t> vec(4);
    std::span<uint8_t> s{ vec };

    auto it = s.rbegin();
    uint32_t stg = data;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it = stg & 0xFF;

    return f.write_data(offset, s);
}

bool write_uint64(random_access_file& f, filesize_t offset, uint64_t data)
{
    std::vector<uint8_t> vec(8);
    std::span<uint8_t> s{ vec };

    auto it = s.rbegin();
    uint64_t stg = data;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it++ = stg & 0xFF;
    stg >>= 8;
    *it = stg & 0xFF;

    return f.write_data(offset, s);
}

bool write_uint8(random_access_file& f, filesize_t offset, uint8_t data)
{
    std::vector<uint8_t> vec(8);
    std::span<uint8_t> s{ vec };

    auto it = s.rbegin();
    *it = data;

    return f.write_data(offset, s);
}
bool write_int32(random_access_file& f, filesize_t offset, int32_t data)
{
    return write_uint32(f, offset, (int32_t)data);
}
bool write_int64(random_access_file& f, filesize_t offset, int64_t data)
{
    return write_uint64(f, offset, (int32_t)data);
}

bool write_int8(random_access_file& f, filesize_t offset, int8_t data)
{
    return write_uint8(f, offset, (int8_t)data);
}

bool write_char(random_access_file& f, filesize_t offset, char data)
{
    return write_uint8(f, offset, (int8_t)data);
}
