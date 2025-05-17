#pragma once

#include "../include/random_access_file.hpp"

#include <vector>
#include <algorithm>
#include <cstring>

class memory_random_access_file : public random_access_file {
public:
    std::vector<uint8_t> buffer;

    filesize_t get_file_size() override {
        return buffer.size();
    }

    void write_data(filesize_t offset, const std::span<uint8_t>& data) override {
        if (offset + data.size() > buffer.size())
            buffer.resize(offset + data.size());

        std::memcpy(buffer.data() + offset, data.data(), data.size());
    }

    void read_data(filesize_t offset, const std::span<uint8_t>& data) override {
        if (offset + data.size() > buffer.size())
            throw object_db_exception("attempt to read past end of buffer");

        std::memcpy(data.data(), buffer.data() + offset, data.size());
    }
};
