#include "../include/file_allocator.hpp"
#include "../include/span_iterator.hpp"
#include "../include/far_offset_ptr.hpp"

static const uint64_t transaction_id_offset = 0;
static const uint64_t transaction_root_offset = transaction_id_offset + sizeof(uint64_t);
static const uint64_t last_transaction_file = transaction_root_offset + sizeof(far_offset_ptr);

concrete_file_allocator::concrete_file_allocator(file_cache& cache) : cache_(cache)
{
}

file_cache& concrete_file_allocator::get_cache() { return cache_; }

filesize_t concrete_file_allocator::get_current_transaction_id()
{
    filesize_t transaction_id = 0;
    std::vector<uint8_t> node(block_size, 0);
    auto root_file_iterator = cache_.get_iterator(0, transaction_id_offset);

    if (cache_.get_file_size(0) >= block_size) // does the block exist on file?
    {
        // load the current value
        auto current_iterator = root_file_iterator;
        read_span(current_iterator, node);
        span_iterator root_block_iterator(node);
        transaction_id = read_filesize(root_block_iterator);
    }
    else // we need to initialise the block with a default state
    {
        // write to the block in memory
        span_iterator root_block_iterator(node);
        write_uint64(root_block_iterator, transaction_id);

        // write the block to the file
        write_span(root_file_iterator, node);
        return transaction_id;
    }

    return transaction_id;
}

filesize_t concrete_file_allocator::create_transaction()
{
    auto transaction_id = get_current_transaction_id();

    std::vector<uint8_t> node(block_size, 0);

    auto root_file_iterator = cache_.get_iterator(0, transaction_id_offset);
    read_span(root_file_iterator, node);

    auto span_it = span_iterator(node);
    transaction_id++;
    write_uint64(span_it, transaction_id);

    root_file_iterator = cache_.get_iterator(0, transaction_id_offset);
    write_span(root_file_iterator, node);

    return transaction_id;
}

far_offset_ptr concrete_file_allocator::allocate_block(filesize_t transaction_id)
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
    if (last_file == 0 || last_file_transaction_id != transaction_id)
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
    auto span_it = span_iterator(block);

    write_filesize(span_it, transaction_id);
    write_span(it, block);

    return far_offset_ptr(last_file, size);
}