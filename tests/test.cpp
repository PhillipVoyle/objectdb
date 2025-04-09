#include "pch.h"
#include <filesystem>
#include "../include/flat_file.hpp"

namespace fs = std::filesystem;

TEST(flat_file, _test_write) {
    fs::path tmp_path{ fs::temp_directory_path() };
    fs::path file_path = tmp_path / "test_write.data";

    // reset
    fs::remove(file_path);

    {
        std_flat_file fout(file_path.string());
        bytevec_t bytes_out{};
        bytes_out.resize(fout.get_blocksize());
        for (int a = 0; a < fout.get_blocksize(); a++)
        {
            bytes_out[a] = uint8_t(a % 256);
        }
        fout.write_block(0, bytes_out);
        for (int a = 0; a < fout.get_blocksize(); a++)
        {
            bytes_out[a] = 77;
        }
        fout.write_block(1, bytes_out);

        std_flat_file fin(file_path.string());
        bytevec_t bytes_in{};
        bytes_in.resize(fin.get_blocksize());
        fin.read_block(0, bytes_in);
        for (int a = 0; a < fin.get_blocksize(); a++)
        {
            EXPECT_EQ(bytes_in[a], uint8_t(a % 256));
        }
        fin.read_block(1, bytes_in);
        for (int a = 0; a < fout.get_blocksize(); a++)
        {
            EXPECT_EQ(bytes_in[a], 77);
        }
    }
    {
        std_flat_file fin(file_path.string());
        bytevec_t bytes_in{};
        bytes_in.resize(fin.get_blocksize());
        fin.read_block(0, bytes_in);
        for (int a = 0; a < fin.get_blocksize(); a++)
        {
            EXPECT_EQ(bytes_in[a], uint8_t(a % 256));
        }
        fin.read_block(1, bytes_in);
        for (int a = 0; a < fin.get_blocksize(); a++)
        {
            EXPECT_EQ(bytes_in[a], 77);
        }
    }

    // reset
    fs::remove(file_path);
}