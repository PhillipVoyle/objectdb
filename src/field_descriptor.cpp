#include "../include/field_descriptor.hpp"
#include "../include/random_access_file.hpp"


void field_descriptor::set_as_integer(uint64_t width)
{
    // we need to be able to fit the width in the length field which is only 60 bits
    if (width > (1ULL << 60) - 1)
    {
        throw object_db_exception("width exceeds maximum allowed value for integer field");
    }
    type_descriptor = (width << 4) | ft_integer;
}
void field_descriptor::set_as_bool()
{
    type_descriptor = ft_bool;
}
void field_descriptor::set_as_text(uint64_t max_length)
{
    // we need to be able to fit the length in the length field which is only 60 bits
    if (max_length > (1ULL << 60) - 1)
    {
        throw object_db_exception("max_length exceeds maximum allowed value for text field");
    }
    type_descriptor = (max_length << 4) | ft_text;
}
void field_descriptor::set_as_binary(uint64_t max_length)
{
    // we need to be able to fit the length in the length field which is only 60 bits
    if (max_length > (1ULL << 60) - 1)
    {
        throw object_db_exception("max_length exceeds maximum allowed value for binary field");
    }
    type_descriptor = (max_length << 4) | ft_binary;
}

void field_descriptor::set_as_real(uint64_t integer_length_bytes, uint64_t fraction_length_bytes)
{
    // we need to be able to fit the lengths in the length field which is only 60 bits
    if (integer_length_bytes > (1ULL << 30) - 1 || fraction_length_bytes > (1ULL << 30) - 1)
    {
        throw object_db_exception("integer_length_bytes or fraction_length_bytes exceeds maximum allowed value for real field");
    }
    type_descriptor = (((integer_length_bytes << 30) | fraction_length_bytes) << 4 )| ft_real;
}

void field_descriptor::set_as_float(uint64_t mantissa_length_bytes, uint64_t exponent_length_bytes)
{
    // we need to be able to fit the lengths in the length field which is only 60 bits
    if (mantissa_length_bytes > (1ULL << 30) - 1 || exponent_length_bytes > (1ULL << 30) - 1)
    {
        throw object_db_exception("mantissa_length_bytes or exponent_length_bytes exceeds maximum allowed value for float field");
    }
    type_descriptor = (((mantissa_length_bytes << 30) | exponent_length_bytes) << 4) | ft_float;
}


void field_descriptor::set_as_date(uint64_t fraction_digits)
{
    // we need to be able to fit the digits in the length field which is only 60 bits
    if (fraction_digits > (1ULL << 60) - 1)
    {
        throw object_db_exception("fraction_digits exceeds maximum allowed value for date field");
    }
    type_descriptor = (fraction_digits << 4) | ft_date;
}

uint64_t field_descriptor::get_type() const
{
    return type_descriptor & 0xF; // last 4 bits are the type
}
uint64_t field_descriptor::get_length() const
{
    return type_descriptor >> 4; // remaining bits are the length
}
uint64_t field_descriptor::get_integer_width() const
{
    if (get_type() != ft_integer)
    {
        throw object_db_exception("field is not an integer type");
    }
    return get_length();
}
uint64_t field_descriptor::get_text_max_length() const
{
    if (get_type() != ft_text)
    {
        throw object_db_exception("field is not a text type");
    }
    return get_length();
}
uint64_t field_descriptor::get_binary_max_length() const
{
    if (get_type() != ft_binary)
    {
        throw object_db_exception("field is not a binary type");
    }
    return get_length();
}
uint64_t field_descriptor::get_date_fraction_digits() const
{
    if (get_type() != ft_date)
    {
        throw object_db_exception("field is not a date type");
    }
    return get_length();
}
std::pair<uint64_t, uint64_t> field_descriptor::get_real_lengths() const
{
    auto type = get_type();
    if ((type != ft_real) && (type != ft_float))
    {
        throw object_db_exception("field is not a real type");
    }
    return { type_descriptor >> 34, (type_descriptor >> 4) & 0x3FFFFFFF }; // high 30 bits are integer length, lower 30 bits are fraction length  
}
std::string field_descriptor::get_type_name() const
{
    switch (get_type())
    {
    case ft_integer: return "integer";
    case ft_bool: return "bool";
    case ft_text: return "text";
    case ft_binary: return "binary";
    case ft_date: return "date";
    case ft_real: return "real";
    case ft_float: return "float";
    default: return "unknown";
    }
}

filesize_t field_descriptor::get_size()
{
    return sizeof(uint64_t) + max_string_length;
}

filesize_t field_descriptor::get_field_width() const
{
    switch (get_type())
    {
    case ft_integer:
        return get_integer_width();
    case ft_bool:
        return 1; // bool is 1 byte
    case ft_text:
    case ft_binary:
        return get_length(); // length is the maximum size
    case ft_date: // date is stored as an iso 8601 string YYYY-MM-DD HH:MM:SS[.fraction]
        return 19 + (get_date_fraction_digits() > 0 ? 1 + get_date_fraction_digits() : 0); // YYYY-MM-DD HH:MM:SS[.fraction]
    case ft_real:
    case ft_float:
    {
        auto lengths = get_real_lengths();
        return lengths.first + lengths.second;
    }
    default:
        throw object_db_exception("unknown field type");
    }
}

void field_descriptor::read_from_span(const std::span<uint8_t>& s)
{
    if (s.size() < get_size())
    {
        throw object_db_exception("field_descriptor: span size is less than expected size");
    }
    auto it = s.begin();
    auto it2 = it + max_string_length;
    auto it3 = it2 + sizeof(uint64_t);

    auto itEnd = it;
    // find the null character if there is one
    while (itEnd != it2)
    {
        if (*itEnd == 0)
        {
            break;
        }
        itEnd++;
    }

    // read the name of the field
    name = std::string(it, itEnd);

    // read the type descriptor
    read_uint64({ it2, it3 }, type_descriptor);
}

void field_descriptor::write_to_span(const std::span<uint8_t>& s) const
{
    if (s.size() < get_size())
    {
        throw object_db_exception("field_descriptor: span size is less than expected size");
    }
    auto it = s.begin();
    auto it2 = it + max_string_length;
    auto it3 = it2 + sizeof(uint64_t);
    // write the name of the field
    std::fill(it, it2, 0); // clear the memory
    std::copy_n(name.begin(), name.length(), it);

    // write the type descriptor
    write_uint64({ it2, it3 }, type_descriptor);
}


field_descriptor create_boolean_field(const std::string& name)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_bool();
    return fd;
}
field_descriptor create_integer_field(const std::string& name, uint64_t width)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_integer(width);
    return fd;
}
field_descriptor create_text_field(const std::string& name, uint64_t max_length)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_text(max_length);
    return fd;
}
field_descriptor create_binary_field(const std::string& name, uint64_t max_length)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_binary(max_length);
    return fd;
}
field_descriptor create_date_field(const std::string& name, uint64_t fraction_digits)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_date(fraction_digits);
    return fd;
}
field_descriptor create_real_field(const std::string& name, uint64_t integer_digits, uint64_t fraction_digits)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_real(integer_digits, fraction_digits);
    return fd;
}

field_descriptor create_float_field(const std::string& name, uint64_t mantissa_digits, uint64_t exponent_digits)
{
    field_descriptor fd;
    fd.name = name;
    fd.set_as_float(mantissa_digits, exponent_digits);
    return fd;
}
