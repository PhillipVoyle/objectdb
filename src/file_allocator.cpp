#include "../include/file_allocator.hpp"
#include "../include/span_iterator.hpp"
#include "../include/far_offset_ptr.hpp"

static const uint64_t transaction_id_offset = 0;
static const uint64_t transaction_root_offset = transaction_id_offset + sizeof(uint64_t);
static const uint64_t last_transaction_file = transaction_root_offset + sizeof(far_offset_ptr);

file_allocator::file_allocator(file_cache& cache) : cache_(cache)
{
}

filesize_t file_allocator::create_transaction()
{
    filesize_t transaction_id = 1;
    std::vector<uint8_t> node(block_size, 0);
    auto root_file_iterator = cache_.get_iterator(0, transaction_id_offset);
    if (cache_.get_file_size(0) >= block_size)
    {
        auto current_iterator = root_file_iterator;
        read_span(current_iterator, node);
        span_iterator root_block_iterator(node);
        transaction_id = read_filesize(root_block_iterator);
    }

    write_span(root_file_iterator, node);
    return transaction_id;
}

far_offset_ptr file_allocator::allocate_block(filesize_t transaction_id)
{
    auto transaction_file_iterator = cache_.get_iterator(0, last_transaction_file);
    if (!transaction_file_iterator.has_next())
    {
        throw object_db_exception("could not read block ptr");
    }

    auto tmp_last_file_iterator = transaction_file_iterator;
    auto last_file = read_uint64(tmp_last_file_iterator);

    auto last_file_transaction_id_iterator = cache_.get_iterator(last_file, 0);
    auto last_file_transaction_id = read_filesize(last_file_transaction_id_iterator);

    uint64_t size = 0;
    if (last_file_transaction_id != transaction_id)
    {
        last_file = last_file + 1;
        write_uint64(transaction_file_iterator, last_file);
    }
    else
    {
        size = cache_.get_file_size(last_file);
        if (size >= block_file_size)
        {
            last_file = last_file + 1;
            write_uint64(transaction_file_iterator, last_file);
            size = 0;
        }
    }

    auto it = cache_.get_iterator(last_file, size);
    std::vector<uint8_t> block(block_size, 0);
    write_span(it, block);

    return far_offset_ptr(last_file, size);
}