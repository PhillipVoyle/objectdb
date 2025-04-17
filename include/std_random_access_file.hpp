#pragma once

#include "../include/random_access_file.hpp"
#include <fstream>
#include <string>

class std_random_access_file : public random_access_file {
public:
    explicit std_random_access_file(const std::string& path);
    ~std_random_access_file();

    filesize_t get_file_size() override;
    bool write_data(filesize_t offset, const std::span<uint8_t>& data) override;
    bool read_data(filesize_t offset, const std::span<uint8_t>& data) override;

private:
    std::fstream file_;
    std::string path_;
    bool is_open_ = false;

    bool ensure_file_open();
};
