#include "pch.h"
#include <filesystem>
#include "../include/logging_btree.hpp"
#include "../include/std_random_access_file.hpp"

namespace fs = std::filesystem;

TEST(logging_btree, create_btree) {
    fs::path tmp_path{ fs::temp_directory_path() };
    fs::path file_path = tmp_path / "test_logging_btree.data";

    // reset
    fs::remove(file_path);


    std_random_access_file f(file_path.string());
    auto end = f.get_file_size();
    logging_btree btree(f);

    filesize_t root_offset = 0;
    EXPECT_TRUE(btree.create_empty_root_node(end, root_offset));
    EXPECT_TRUE(write_filesize(f, root_offset, end));

    std::vector<uint8_t> data(16);
    std::span<uint8_t> s{ data };
    EXPECT_TRUE(f.read_data(0, s));
}
