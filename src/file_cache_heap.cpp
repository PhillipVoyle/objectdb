#include <array>
#include <cstring>
#include "../include/file_cache_heap.hpp"
#include "../include/span_iterator.hpp"

file_cache_heap::file_cache_heap(file_allocator& allocator, far_offset_ptr heap_root)
    : heap_root_(heap_root), allocator_(allocator)
{
}

far_offset_ptr file_cache_heap::heap_allocate()
{
    if (!heap_root_) {
        // No root: allocate a new block and build a free list from the entries
        filesize_t txn_id = allocator_.get_current_transaction_id();
        far_offset_ptr block_ptr = allocator_.allocate_block(txn_id);

        // Build free list: each entry points to the next, last entry points to null
        std::vector<uint8_t> block_data(BLOCK_SIZE, 0);
        for (size_t i = 0; i < ENTRIES_PER_BLOCK; ++i) {
            size_t entry_offset = i * ENTRY_SIZE;

            bool has_next = (i + 1 < ENTRIES_PER_BLOCK) || (i == 0);

            // place the far_offset_ptr at the end of the entry

            if (has_next)
            {
                far_offset_ptr next_ptr(block_ptr.get_file_id(), block_ptr.get_offset() + (i + 1) * ENTRY_SIZE);
                span_iterator sit{ std::span<uint8_t>{ block_data.data() + entry_offset + ENTRY_SIZE - far_offset_ptr::get_size(), far_offset_ptr::get_size() } };
                next_ptr.write(sit); // write the next pointer
            }
        }

        // Write the block to the file
        allocator_.get_cache().write_bytes(block_ptr.get_file_id(), block_ptr.get_offset(), block_data);

        // Set heap_root_ to the second_entry;
        heap_root_ = far_offset_ptr(block_ptr.get_file_id(), block_ptr.get_offset() + ENTRY_SIZE);

        return block_ptr; // return the first entry
    }
    else
    {
        // Read the root entry to get the next free entry
        auto entry = read_heap(heap_root_);

        // Update the free list root to the next entry
        span_iterator it{ std::span<uint8_t>{ entry.data() + ENTRY_SIZE - far_offset_ptr::get_size(), far_offset_ptr::get_size() }};
        far_offset_ptr next_free;
        next_free.read(it); // read the far_offset_ptr at the end of the entry
        far_offset_ptr allocated_entry = heap_root_;
        heap_root_ = next_free; // update root to next free entry
        return allocated_entry;
    }
}

void file_cache_heap::heap_free(far_offset_ptr location)
{
    // Read the entry at location to ensure it's valid (optional)
    std::vector<uint8_t> entry(ENTRY_SIZE, 0);
    // Write the current heap_root_ as the next pointer in this entry
    span_iterator it{ std::span<uint8_t>{ entry.data() + ENTRY_SIZE - far_offset_ptr::get_size(), far_offset_ptr::get_size() }};
    heap_root_.write(it); // write the current root as the next pointer
    // Write the updated entry back to the file
    write_heap(location, entry);
    // Update heap_root_ to point to this newly freed entry
    heap_root_ = location;
}

std::vector<uint8_t> file_cache_heap::read_heap(far_offset_ptr location)
{
    std::vector<uint8_t> entry(ENTRY_SIZE);
    allocator_.get_cache().read_bytes(location.get_file_id(), location.get_offset(), entry);
    return entry;
}

void file_cache_heap::write_heap(far_offset_ptr location, std::span<uint8_t> data)
{
    if (data.size() != ENTRY_SIZE)
        throw object_db_exception("heap entry must be 256 bytes");

    allocator_.get_cache().write_bytes(location.get_file_id(), location.get_offset(), data);
}
