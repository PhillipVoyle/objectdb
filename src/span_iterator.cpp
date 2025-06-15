
#include <span>
#include <cstddef>
#include <cstdint>
#include "../include/core.hpp"
#include "../include/span_iterator.hpp"

span_iterator::span_iterator(std::span<uint8_t> data, std::size_t offset)
    : data_(data), offset_(offset) {}

uint8_t span_iterator::read() {
    if (offset_ >= data_.size()) {
        throw object_db_exception("span_iterator::read: out of range");
    }
    return data_[offset_++];
}

void span_iterator::write(uint8_t value) {
    if (offset_ >= data_.size()) {
        throw object_db_exception("span_iterator::write: out of range");
    }
    data_[offset_++] = value;
}

bool span_iterator::has_next() const {
    return offset_ < data_.size();
}
