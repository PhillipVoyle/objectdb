#pragma once

#include "../include/core.hpp"

enum field_type
{
    ft_integer = 0,
    ft_bool = 1,
    ft_text = 2,
    ft_binary = 3,
    ft_date = 4,
    ft_real = 5,
    ft_float = 6
};

struct field_descriptor
{
    static const int max_string_length = 64; // maximum length for field names
    std::string name;

    /// <summary>
    /// packed type descriptor and flags
    ///
    /// Layout Diagram:
    /// +----------------------------+--------------------+
    /// | 63 ...                   4 | 3 ...            0 |
    /// +----------------------------+--------------------+
    /// | Lengths                    | Field Type (4 bits)|
    /// +----------------------------+--------------------+
    ///  
    /// - Field Type (4 bits):
    ///   - Represents the type of the field (e.g., integer, bool, text, etc.).
    /// - Lengths (remaining bits):
    ///   - Text/Binary: Maximum length of the field.
    ///   - Date: Number of digits in the fraction of the seconds field.
    ///   - Real: Two 30-bit lengths:
    ///     - High 30 bits: Bytes in the integer part.
    ///     - Low 30 bits: Bytes in the fraction part.
    ///   - Float: Two 30-bit lengths:
    ///     - High 30 bits: Bytes in the mantissa
    ///     - Low 30 bits: Bytes in the exponent
    /// </summary>
    uint64_t type_descriptor;

    void set_as_integer(uint64_t width = 0);
    void set_as_bool();
    void set_as_text(uint64_t max_length = 0);
    void set_as_binary(uint64_t max_length = 0);

    void set_as_real(uint64_t integer_length_bytes = 0, uint64_t fraction_length_bytes = 0);
    void set_as_float(uint64_t mantissa_length_bytes = 0, uint64_t exponent_length_bytes = 0);
    void set_as_date(uint64_t fraction_digits = 0);
    uint64_t get_type() const;
    uint64_t get_length() const;
    uint64_t get_integer_width() const;
    uint64_t get_text_max_length() const;
    uint64_t get_binary_max_length() const;
    uint64_t get_date_fraction_digits() const;
    std::pair<uint64_t, uint64_t> get_real_lengths() const;
    std::string get_type_name() const;

    filesize_t get_size() const;
    filesize_t get_field_width() const;
    void read_from_span(const std::span<uint8_t>& s);
    void write_to_span(const std::span<uint8_t>& s) const;
};


field_descriptor create_boolean_field(const std::string& name);
field_descriptor create_integer_field(const std::string& name, uint64_t width = 0);
field_descriptor create_text_field(const std::string& name, uint64_t max_length = 0);
field_descriptor create_binary_field(const std::string& name, uint64_t max_length = 0);
field_descriptor create_date_field(const std::string& name, uint64_t fraction_digits = 0);
field_descriptor create_real_field(const std::string& name, uint64_t integer_digits = 0, uint64_t fraction_digits = 0);
field_descriptor create_float_field(const std::string& name, uint64_t mantissa_digits = 0, uint64_t exponent_digits = 0);
