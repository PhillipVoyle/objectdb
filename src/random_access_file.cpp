#include "../include/random_access_file.hpp"
#include <vector>
#include <span>
#include <cassert>


void read_uint32(random_access_file& f, filesize_t offset, uint32_t& data)
{
    std::vector<uint8_t> vec(4);
    std::span<uint8_t> s(vec);
    assert(s.size() == 4);
    assert(s.size_bytes() == 4);
    f.read_data(offset, s);

    read_uint32(s, data);
}


void read_uint64(random_access_file& f, filesize_t offset, uint64_t& data)
{
    std::vector<uint8_t> vec(8);
    assert(vec.size() == 8);
    f.read_data(offset, vec);

    read_uint64(vec, data);
}

void read_uint8(random_access_file& f, filesize_t offset, uint8_t& data)
{
    std::vector<uint8_t> vec(1);
    assert(vec.size() == 1);
    f.read_data(offset, vec);

    read_uint8(vec, data);
}

void read_int32(random_access_file& f, filesize_t offset, int32_t& data)
{
    uint32_t stg = 0;
    read_uint32(f, offset, stg);
    data = stg;
}

void read_int64(random_access_file& f, filesize_t offset, int64_t& data)
{
    uint64_t stg = 0;
    read_uint64(f, offset, stg);
    data = stg;
}

void read_int8(random_access_file& f, filesize_t offset, int8_t& data)
{
    uint8_t stg = 0;
    read_uint8(f, offset, stg);
    data = stg;
}

void read_char(random_access_file& f, filesize_t offset, char& data)
{
    uint8_t stg = 0;
    read_uint8(f, offset, stg);
    data = stg;
}

void write_uint32(random_access_file& f, filesize_t offset, uint32_t data)
{
    std::vector<uint8_t> vec(4);
    assert(vec.size() == 4);
    write_uint32(vec, data);
    f.write_data(offset, vec);
}

void write_uint64(random_access_file& f, filesize_t offset, uint64_t data)
{
    std::vector<uint8_t> vec(8);
    write_uint64(vec, data);
    f.write_data(offset, vec);
}

void write_uint8(random_access_file& f, filesize_t offset, uint8_t data)
{
    std::vector<uint8_t> vec(1);
    assert(vec.size() == 1);
    vec[0] = data;

    f.write_data(offset, vec);
}

void write_int32(random_access_file& f, filesize_t offset, int32_t data)
{
    write_uint32(f, offset, (int32_t)data);
}
void write_int64(random_access_file& f, filesize_t offset, int64_t data)
{
    write_uint64(f, offset, (int32_t)data);
}

void write_int8(random_access_file& f, filesize_t offset, int8_t data)
{
    write_uint8(f, offset, (int8_t)data);
}

void write_char(random_access_file& f, filesize_t offset, char data)
{
    write_uint8(f, offset, (int8_t)data);
}

void read_uint32(const std::span<uint8_t>& memory, uint32_t& data)
{
    if (4 > memory.size())
    {
        throw object_db_exception("read_uint32: memory size is less than 4");
    }

    uint32_t result = 0;
    auto it = memory.begin();

    result = *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it++;
    result <<= 8;
    result |= *it;

    data = result;
}

void read_uint64(const std::span<uint8_t>& memory, uint64_t& data)
{
    if (8 > memory.size())
    {
        throw object_db_exception("read_uint64: memory size is less than 8");
    }

    uint64_t result = 0;
    auto it = memory.begin();

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
}

void read_uint8(const std::span<uint8_t>& memory, uint8_t& data)
{
    if (1 > memory.size())
    {
        throw object_db_exception("read_uint8: memory size is less than 1");
    }

    data = memory[0];
}

void read_int32(const std::span<uint8_t>& memory, int32_t& data)
{
    uint32_t stg = 0;
    read_uint32(memory, stg);
    data = static_cast<int32_t>(stg);
}

void read_int64(const std::span<uint8_t>& memory, int64_t& data)
{
    uint64_t stg = 0;
    read_uint64(memory, stg);
    data = static_cast<int64_t>(stg);
}

void read_int8(const std::span<uint8_t>& memory, int8_t& data)
{
    uint8_t stg = 0;
    read_uint8(memory, stg);
    data = static_cast<int8_t>(stg);
}

void read_char(const std::span<uint8_t>& memory, char& data)
{
    uint8_t stg = 0;
    read_uint8(memory, stg);
    data = static_cast<char>(stg);
}

void write_uint32(const std::span<uint8_t>& memory, uint32_t data)
{
    if (4 > memory.size())
    {
        throw object_db_exception("write_uint32: memory size is less than 4");
    }

    auto it = memory.begin();
    uint32_t stg = data;
    *it++ = (stg >> 24) & 0xFF;
    *it++ = (stg >> 16) & 0xFF;
    *it++ = (stg >> 8) & 0xFF;
    *it = stg & 0xFF;
}

void write_uint64(const std::span<uint8_t>& memory, uint64_t data)
{
    if (8 > memory.size())
    {
        throw object_db_exception("write_uint64: memory size is less than 8");
    }

    auto it = memory.begin();
    uint64_t stg = data;
    *it++ = (stg >> 56) & 0xFF;
    *it++ = (stg >> 48) & 0xFF;
    *it++ = (stg >> 40) & 0xFF;
    *it++ = (stg >> 32) & 0xFF;
    *it++ = (stg >> 24) & 0xFF;
    *it++ = (stg >> 16) & 0xFF;
    *it++ = (stg >> 8) & 0xFF;
    *it = stg & 0xFF;
}

void write_uint8(const std::span<uint8_t>& memory, uint8_t data)
{
    if (1 > memory.size())
    {
        throw object_db_exception("write_uint8: memory size is less than 1");
    }

    memory[0] = data;
}

void write_int32(const std::span<uint8_t>& memory, int32_t data)
{
    write_uint32(memory, static_cast<uint32_t>(data));
}

void write_int64(const std::span<uint8_t>& memory, int64_t data)
{
    write_uint64(memory, static_cast<uint64_t>(data));
}

void write_int8(const std::span<uint8_t>& memory, int8_t data)
{
    write_uint8(memory, static_cast<uint8_t>(data));
}

void write_char(const std::span<uint8_t>& memory, char data)
{
    write_uint8(memory, static_cast<uint8_t>(data));
}
