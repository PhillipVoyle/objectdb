#include "../include/std_random_access_file.hpp"
#include <filesystem>

std_random_access_file::std_random_access_file(const std::string& path)
    : path_(path)
{
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        // Attempt to create the file if it doesn't exist
        file_.clear();
        file_.open(path_, std::ios::out | std::ios::binary);
        file_.close();
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    is_open_ = file_.is_open();
}

std_random_access_file::~std_random_access_file() {
    if (file_.is_open()) {
        file_.close();
    }
}

filesize_t std_random_access_file::get_file_size() {
    if (!is_open_) return 0;
    auto current_pos = file_.tellg();
    file_.seekg(0, std::ios::end);
    filesize_t size = static_cast<filesize_t>(file_.tellg());
    file_.seekg(current_pos);
    return size;
}

void std_random_access_file::write_data(filesize_t offset, const std::span<uint8_t>& data) {
    if (!is_open_) throw object_db_exception("File is not open");

    file_.seekp(offset, std::ios::beg);
    if (!file_.good()) throw object_db_exception("Failed to seek to offset");

    file_.write(reinterpret_cast<const char*>(data.data()), data.size());
    if (!file_.good())
    {
        throw object_db_exception("Failed to write data");
    }
}

void std_random_access_file::read_data(filesize_t offset, const std::span<uint8_t>& data) {
    if (!is_open_)
    {
        throw object_db_exception("File is not open");
    }

    file_.seekg(offset, std::ios::beg);
    if (!file_.good())
    {
        throw object_db_exception("Failed to seek to offset");
    }

    file_.read(reinterpret_cast<char*>(data.data()), data.size());
    if (!file_.good())
    {
        throw object_db_exception("Failed to read data");
    }
}
