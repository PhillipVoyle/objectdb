
#include "../include/btree.hpp"

btree::btree(file_cache& cache, far_offset_ptr offset) : cache_(cache), offset_(offset)
{
}

btree_iterator btree::seek_begin(std::span<uint8_t> key)
{
    return btree_operations::seek_begin(cache_, offset_, key);
}
btree_iterator btree::seek_end(std::span<uint8_t> key)
{
    return btree_operations::seek_end(cache_, offset_, key);
}

btree_iterator btree::next(btree_iterator it)
{
    return btree_operations::next(cache_, it);
}
btree_iterator btree::prev(btree_iterator it)
{
    return btree_operations::prev(cache_, it);
}

btree_iterator btree::upsert(std::span<uint8_t> entry)
{
    return btree_operations::upsert(cache_, offset_, entry);
}

std::vector<uint8_t> btree::get_entry(btree_iterator it)
{
    return btree_operations::get_entry(cache_, it);
}

btree_iterator btree::insert(btree_iterator it, std::span<uint8_t> entry)
{
    return btree_operations::insert(cache_, it, entry);
}
btree_iterator btree::update(btree_iterator it, std::span<uint8_t> entry)
{
    return btree_operations::update(cache_, it, entry);
}
btree_iterator btree::remove(btree_iterator it)
{
    return btree_operations::remove(cache_, it);
}

btree_iterator btree_operations::seek_begin(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key)
{
    btree_iterator result;
    btree_node node;
    auto current_offset = btree_offset;
    for (;;)
    {
        auto iterator = cache.get_iterator(current_offset.get_file_id(), current_offset.get_offset());
        node.read(iterator);
        auto md = node.get_metadata();
        auto find_result = node.find_key(md, key);
        btree_node_info info;
        info.node_offset = current_offset;
        info.btree_position = find_result.position;
        info.found = find_result.found;
        auto span = node.get_key_at(find_result.position);
        info.key.resize(span.size());
        std::copy(span.begin(), span.end(), info.key.begin());

        result.path.push_back(info);

        if (node.is_leaf())
        {
            break;
        }
        else
        {
            node.get_value_at(find_result.position);
        }
    }
    return result;
}

btree_iterator btree_operations::seek_end(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> key)
{
    throw object_db_exception("not implemented");
}

btree_iterator btree_operations::next(file_cache& cache, btree_iterator it)
{
    throw object_db_exception("not implemented");
}
btree_iterator btree_operations::prev(file_cache& cache, btree_iterator it)
{
    throw object_db_exception("not implemented");
}

btree_iterator btree_operations::upsert(file_cache& cache, far_offset_ptr btree_offset, std::span<uint8_t> entry)
{
    throw object_db_exception("not implemented");
}

std::vector<uint8_t> btree_operations::get_entry(file_cache& cache, btree_iterator it)
{
    throw object_db_exception("not implemented");
}

btree_iterator btree_operations::insert(file_cache& cache, btree_iterator it, std::span<uint8_t> entry)
{
    throw object_db_exception("not implemented");
}
btree_iterator btree_operations::update(file_cache& cache, btree_iterator it, std::span<uint8_t> entry)
{
    throw object_db_exception("not implemented");
}
btree_iterator btree_operations::remove(file_cache& cache, btree_iterator it)
{
    throw object_db_exception("not implemented");
}
