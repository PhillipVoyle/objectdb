#include "../include/file_cache_heap.hpp"

file_cache_heap::file_cache_heap(file_allocator& allocator, far_offset_ptr heap_root) : allocator_(allocator), heap_root_(heap_root)
{
}

far_offset_ptr file_cache_heap::heap_allocate()
{
    throw object_db_exception("not implemented");
}

void file_cache_heap::heap_free(far_offset_ptr location)
{
}

std::vector<uint8_t> file_cache_heap::read_heap(far_offset_ptr location)
{
    throw object_db_exception("not implemented");
}
void file_cache_heap::write_heap(far_offset_ptr location, std::span<uint8_t>)
{
}
