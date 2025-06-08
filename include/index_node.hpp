#pragma once

#include <vector>
#include <cstdint>
#include <memory>
#include <span>
#include <cstring>
#include "../include/offset_ptr.hpp"

template<typename entry_type>
class IIndexNode;

// Entry type for branch nodes: key + far_offset_ptr
template<typename key_type, typename entry_type>
struct branch_entry {
    key_type key;
    far_offset_ptr<IIndexNode<entry_type>> child_ptr;

    filesize_t get_size() const {
        return key.size() + child_ptr.get_size();
    }

    void read_from_span(const std::span<uint8_t>& s) {
        auto it = s.begin();
        key.resize(key.size());
        std::memcpy(key.data(), &(*it), key.size());
        it += key.size();
        child_ptr.read_from_span({ it, it + child_ptr.get_size() });
    }

    void write_to_span(const std::span<uint8_t>& s) const {
        auto it = s.begin();
        std::memcpy(&(*it), key.data(), key.size());
        it += key.size();
        child_ptr.write_to_span({ it, it + child_ptr.get_size() });
    }
};

// Common interface for all index nodes
template<typename entry_type>
class IIndexNode {
public:
    using key_type = std::vector<uint8_t>;
    using value_type = std::vector<uint8_t>;

    virtual ~IIndexNode() = default;
    virtual bool is_leaf() const = 0;
    virtual size_t entry_count() const = 0;
    virtual const entry_type& get_entry(size_t idx) const = 0;
    virtual void add_entry(const entry_type& entry) = 0;
    virtual void set_entries(const std::vector<entry_type>& entries) = 0;
    virtual const std::vector<entry_type>& get_entries() const = 0;
    virtual filesize_t get_size() const = 0;
    virtual void read_from_span(const std::span<uint8_t>& s) = 0;
    virtual void write_to_span(const std::span<uint8_t>& s) const = 0;
    virtual void read_object(far_offset_ptr<IIndexNode<entry_type>>& ptr, file_ref_cache& ref_cache) = 0;
    virtual void write_object(far_offset_ptr<IIndexNode<entry_type>>& ptr, file_ref_cache& ref_cache) = 0;
};

// Common implementation for both leaf and branch nodes
template<typename entry_type>
class index_node_impl : public IIndexNode<entry_type> {
protected:
    std::vector<entry_type> entries_;
    bool is_leaf_;
public:
    index_node_impl(bool is_leaf) : is_leaf_(is_leaf) {}

    bool is_leaf() const override { return is_leaf_; }
    size_t entry_count() const override { return entries_.size(); }
    const entry_type& get_entry(size_t idx) const override { return entries_[idx]; }
    void add_entry(const entry_type& entry) override { entries_.push_back(entry); }
    void set_entries(const std::vector<entry_type>& entries) override { entries_ = entries; }
    const std::vector<entry_type>& get_entries() const override { return entries_; }

    filesize_t get_size() const override {
        filesize_t size = sizeof(uint8_t); // is_leaf_
        size += sizeof(uint32_t); // entry count
        for (const auto& entry : entries_) {
            size += entry.get_size();
        }
        return size;
    }

    void read_from_span(const std::span<uint8_t>& s) override {
        auto it = s.begin();
        is_leaf_ = (*it != 0);
        ++it;
        uint32_t entry_count = 0;
        std::memcpy(&entry_count, &(*it), sizeof(uint32_t));
        it += sizeof(uint32_t);

        entries_.clear();
        for (uint32_t i = 0; i < entry_count; ++i) {
            entry_type entry;
            auto entry_size = entry.get_size();
            entry.read_from_span({ it, it + entry_size });
            entries_.push_back(entry);
            it += entry_size;
        }
    }

    void write_to_span(const std::span<uint8_t>& s) const override {
        auto it = s.begin();
        *it = is_leaf_ ? 1 : 0;
        ++it;
        uint32_t entry_count = static_cast<uint32_t>(entries_.size());
        std::memcpy(&(*it), &entry_count, sizeof(uint32_t));
        it += sizeof(uint32_t);

        for (const auto& entry : entries_) {
            auto entry_size = entry.get_size();
            entry.write_to_span({ it, it + entry_size });
            it += entry_size;
        }
    }

    void read_object(far_offset_ptr<IIndexNode<entry_type>>& ptr, file_ref_cache& ref_cache) override {
        auto file = ref_cache.acquire_file(ptr.get_filename());
        std::vector<uint8_t> data(get_size(), 0);
        file->read_data(ptr.get_offset(), data);
        read_from_span({ data.data(), data.data() + data.size() });
    }

    void write_object(far_offset_ptr<IIndexNode<entry_type>>& ptr, file_ref_cache& ref_cache) override {
        auto file = ref_cache.acquire_file(ptr.get_filename());
        std::vector<uint8_t> data(get_size(), 0);
        write_to_span({ data.data(), data.data() + data.size() });
        file->write_data(ptr.get_offset(), data);
    }
};

// Leaf node
template<typename entry_type>
class index_leaf_node : public index_node_impl<entry_type> {
public:
    index_leaf_node() : index_node_impl<entry_type>(true) {}
};

// Branch node is a special case of leaf node with branch_entry
template<typename key_type, typename entry_type>
class index_branch_node : public index_node_impl<branch_entry<key_type, entry_type>> {
public:
    using branch_entry_type = branch_entry<key_type, entry_type>;
    index_branch_node() : index_node_impl<branch_entry_type>(false) {}
};

template<typename entry_type>
static std::unique_ptr<IIndexNode<entry_type>> read_node_from_span(const std::span<uint8_t>& s) {
    if (s.empty()) throw std::runtime_error("Empty span for node deserialization");
    bool is_leaf = s[0];
    std::unique_ptr<IIndexNode<entry_type>> node;
    if (is_leaf) {
        node = std::make_unique<index_leaf_node<entry_type>>();
    } else {
        node = std::make_unique<index_branch_node<typename entry_type::key_type>>();
    }
    node->read_from_span(s);
    return node;
}

template<typename entry_type>
static std::unique_ptr<IIndexNode<entry_type>> read_node_object(
    far_offset_ptr<IIndexNode<entry_type>>& ptr,
    file_ref_cache& ref_cache)
{
    auto file = ref_cache.acquire_file(ptr.get_filename());

    // Read the first byte to determine node type
    uint8_t type_byte = 0;
    std::vector<uint8_t> type_buf(1, 0);
    file->read_data(ptr.get_offset(), std::span<uint8_t>(type_buf.data(), 1));
    type_byte = type_buf[0];

    // Create a temporary node to determine size
    std::unique_ptr<IIndexNode<entry_type>> temp_node;
    if (type_byte) {
        temp_node = std::make_unique<index_leaf_node<entry_type>>();
    } else {
        temp_node = std::make_unique<index_branch_node<typename entry_type::key_type>>();
    }
    filesize_t node_size = temp_node->get_size();

    // Read the full node data
    std::vector<uint8_t> data(node_size, 0);
    file->read_data(ptr.get_offset(), std::span<uint8_t>(data.data(), node_size));

    // Use the span factory to create the correct node
    return read_node_from_span<entry_type>(std::span<uint8_t>(data.data(), node_size));
}

