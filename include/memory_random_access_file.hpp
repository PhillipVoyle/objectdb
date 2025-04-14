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

    bool write_data(filesize_t offset, const std::span<uint8_t>& data) override {
        if (offset + data.size() > buffer.size())
            buffer.resize(offset + data.size());

        std::memcpy(buffer.data() + offset, data.data(), data.size());
        return true;
    }

    bool read_data(filesize_t offset, std::span<uint8_t>& data) override {
        if (offset + data.size() > buffer.size())
            return false;

        std::memcpy(data.data(), buffer.data() + offset, data.size());
        return true;
    }
};
