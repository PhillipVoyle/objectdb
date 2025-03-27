#include "flat_file.hpp"
#include <iostream>
#include <filesystem>

void std_flat_file::invariant() const
{
    assert(_block_size > 512);
    assert(_file_size >= 0);
    assert((_file_size % _block_size) == 0);
}

std_flat_file::std_flat_file(const std::string& filename)
{
    _file.imbue(std::locale::classic());
    _file.open(filename, std::ios_base::binary | std::ios_base::out | std::ios_base::in);
    if (!_file.is_open())
    {
        _file.open(filename, std::ios_base::trunc | std::ios_base::binary| std::ios_base::in | std::ios_base::out);
    }
    _file.seekp(0, std::ios::end);
    _file_size = _file.tellp();

    _file.seekg(0);
    invariant();
}


filesize_t std_flat_file::get_blocksize()
{
    return _block_size;
}

int std_flat_file::read_block(filesize_t block_nr, bytevec_t& block)
{
    invariant();

    if ((block_nr < 0) || ((block_nr + 1) * _block_size > _file_size))
    {
        return E_INVALID_BLOCK;
    }

    if (_file.tellg() != (block_nr * _block_size))
    {
        _file.seekg(block_nr * _block_size, std::ios::beg);
    }

    block.resize(0);
    block.reserve(_block_size);

    std::copy_n(std::istreambuf_iterator<uint8_t>(_file), _block_size, std::back_inserter(block));
    block.shrink_to_fit();

    if (_file.fail())
    {
        return E_INVALID_BLOCK;
    }

    return S_OK;
}

int std_flat_file::write_block(filesize_t block_nr, const bytevec_t& block)
{
    invariant();

    auto candidate_position = block_nr * _block_size;

    if ((block_nr < 0) || (candidate_position > _file_size))
    {
        return E_INVALID_BLOCK;
    }
    if (block.size() != _block_size)
    {
        return E_INVALID_BLOCK_SIZE;
    }

    if (_file.tellp() != candidate_position)
    {
        _file.seekp(candidate_position, std::ios::beg);
    }
    _file.write(block.data(), block.size());

    if (_file.fail())
    {
        return E_INVALID_BLOCK;
    }

    if (candidate_position == _file_size)
    {
        _file_size += _block_size;
        assert(_file.tellp() == _file_size);
    }

    invariant();

    return S_OK;
}

