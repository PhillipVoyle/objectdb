#pragma once
#include <fstream>
#include <vector>
#include <cstdint>
#include <string>
#include <algorithm>
#include <iterator>
#include <cassert>

static const int S_OK = 0x00000000;
static const int E_INVALID_BLOCK = 0xF1000001;
static const int E_INVALID_BLOCK_SIZE = 0xF1000002;

using bytevec_t = std::vector<std::uint8_t>;
using filesize_t = std::int64_t;

class flat_file
{
public:
    virtual filesize_t get_blocksize() = 0;
    virtual filesize_t get_file_size() = 0;
    virtual int read_block(filesize_t block_nr, bytevec_t& block) = 0;
    virtual int write_block(filesize_t block_nr, const bytevec_t& block) = 0;
    virtual ~flat_file() = default;
};

class std_flat_file : public flat_file
{
    std::basic_fstream<uint8_t> _file;
    filesize_t _block_size = 8192;
    filesize_t _file_size;

    void invariant() const;

public:
    explicit std_flat_file(const std::string& filename);

    ~std_flat_file() final = default;

    filesize_t get_blocksize() final;
    filesize_t get_file_size() final;

    int read_block(filesize_t block_nr, bytevec_t& block) final;

    int write_block(filesize_t block_nr, const bytevec_t& block) final;
};

