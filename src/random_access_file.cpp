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

    return read_uint32(s, 0, data);
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

    return read_uint64(s, 0, data);
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

    return read_uint8(s, 0, data);
}

bool read_int32(random_access_file& f, filesize_t offset, int32_t& data)
{
    if (uint32_t stg = 0; read_uint32(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool read_int64(random_access_file& f, filesize_t offset, int64_t& data)
{
    if (uint64_t stg = 0; read_uint64(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool read_int8(random_access_file& f, filesize_t offset, int8_t& data)
{
    if (uint8_t stg = 0; read_uint8(f, offset, stg))
    {
        data = stg;
        return true;
    }
    return false;
}

bool read_char(random_access_file& f, filesize_t offset, char& data)
{
    if (uint8_t stg = 0; read_uint8(f, offset, stg))
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

    assert(s.size() == 4);

    if (!write_uint32(s, 0, data))
    {
        return false;
    }
    return f.write_data(offset, s);
}

bool write_uint64(random_access_file& f, filesize_t offset, uint64_t data)
{
    std::vector<uint8_t> vec(8);
    std::span<uint8_t> s{ vec };

    if (!write_uint64(s, 0, data))
    {
        return false;
    }

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


bool read_uint32(const std::span<const uint8_t>& memory, filesize_t offset, uint32_t& data)
{
    if (offset + 4 > memory.size())
    {
        return false;
    }

    uint32_t result = 0;
    auto it = memory.begin() + offset;

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

bool read_uint64(const std::span<const uint8_t>& memory, filesize_t offset, uint64_t& data)
{
    if (offset + 8 > memory.size())
    {
        return false;
    }

    uint64_t result = 0;
    auto it = memory.begin() + offset;

    result = *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it;

    data = result;
    return true;
}

bool read_uint8(const std::span<const uint8_t>& memory, filesize_t offset, uint8_t& data)
{
    if (offset + 1 > memory.size())
    {
        return false;
    }

    data = memory[offset];
    return true;
}

bool read_int32(const std::span<const uint8_t>& memory, filesize_t offset, int32_t& data)
{
    if (uint32_t stg = 0; read_uint32(memory, offset, stg))
    {
        data = static_cast<int32_t>(stg);
        return true;
    }
    return false;
}

bool read_int64(const std::span<const uint8_t>& memory, filesize_t offset, int64_t& data)
{
    if (uint64_t stg = 0; read_uint64(memory, offset, stg))
    {
        data = static_cast<int64_t>(stg);
        return true;
    }
    return false;
}

bool read_int8(const std::span<const uint8_t>& memory, filesize_t offset, int8_t& data)
{
    if (uint8_t stg = 0; read_uint8(memory, offset, stg))
    {
        data = static_cast<int8_t>(stg);
        return true;
    }
    return false;
}

bool read_char(const std::span<const uint8_t>& memory, filesize_t offset, char& data)
{
    if (uint8_t stg = 0; read_uint8(memory, offset, stg))
    {
        data = static_cast<char>(stg);
        return true;
    }
    return false;
}

bool write_uint32(std::span<uint8_t>& memory, filesize_t offset, uint32_t data)
{
    if (offset + 4 > memory.size())
    {
        return false;
    }

    auto it = memory.begin() + offset;
    uint32_t stg = data;
    *it++ = (stg >> 24) & 0xFF;
    *it++ = (stg >> 16) & 0xFF;
    *it++ = (stg >> 8) & 0xFF;
    *it = stg & 0xFF;

    return true;
}

bool write_uint64(std::span<uint8_t>& memory, filesize_t offset, uint64_t data)
{
    if (offset + 8 > memory.size())
    {
        return false;
    }

    auto it = memory.begin() + offset;
    uint64_t stg = data;
    *it++ = (stg >> 56) & 0xFF;
    *it++ = (stg >> 48) & 0xFF;
    *it++ = (stg >> 40) & 0xFF;
    *it++ = (stg >> 32) & 0xFF;
    *it++ = (stg >> 24) & 0xFF;
    *it++ = (stg >> 16) & 0xFF;
    *it++ = (stg >> 8) & 0xFF;
    *it = stg & 0xFF;

    return true;
}

bool write_uint8(std::span<uint8_t>& memory, filesize_t offset, uint8_t data)
{
    if (offset + 1 > memory.size())
    {
        return false;
    }

    memory[offset] = data;
    return true;
}

bool write_int32(std::span<uint8_t>& memory, filesize_t offset, int32_t data)
{
    return write_uint32(memory, offset, static_cast<uint32_t>(data));
}

bool write_int64(std::span<uint8_t>& memory, filesize_t offset, int64_t data)
{
    return write_uint64(memory, offset, static_cast<uint64_t>(data));
}

bool write_int8(std::span<uint8_t>& memory, filesize_t offset, int8_t data)
{
    return write_uint8(memory, offset, static_cast<uint8_t>(data));
}

bool write_char(std::span<uint8_t>& memory, filesize_t offset, char data)
{
    return write_uint8(memory, offset, static_cast<uint8_t>(data));
}
