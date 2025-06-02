#include "pch.h"
#include "../include/field_descriptor.hpp"
#include <gtest/gtest.h>
#include <string>

TEST(FieldDescriptorTests, Create_Types) {
    // Integer field
    field_descriptor int_fd = create_integer_field("int_field", 8);
    EXPECT_EQ(int_fd.name, "int_field");
    EXPECT_EQ(int_fd.get_type(), ft_integer);
    EXPECT_EQ(int_fd.get_integer_width(), 8);

    // Boolean field
    field_descriptor bool_fd = create_boolean_field("bool_field");
    EXPECT_EQ(bool_fd.name, "bool_field");
    EXPECT_EQ(bool_fd.get_type(), ft_bool);

    // Text field
    field_descriptor text_fd = create_text_field("text_field", 32);
    EXPECT_EQ(text_fd.name, "text_field");
    EXPECT_EQ(text_fd.get_type(), ft_text);
    EXPECT_EQ(text_fd.get_text_max_length(), 32);

    // Binary field
    field_descriptor bin_fd = create_binary_field("bin_field", 16);
    EXPECT_EQ(bin_fd.name, "bin_field");
    EXPECT_EQ(bin_fd.get_type(), ft_binary);
    EXPECT_EQ(bin_fd.get_binary_max_length(), 16);

    // Date field
    field_descriptor date_fd = create_date_field("date_field", 3);
    EXPECT_EQ(date_fd.name, "date_field");
    EXPECT_EQ(date_fd.get_type(), ft_date);
    EXPECT_EQ(date_fd.get_date_fraction_digits(), 3);

    // Real field
    field_descriptor real_fd = create_real_field("real_field", 4, 2);
    EXPECT_EQ(real_fd.name, "real_field");
    EXPECT_EQ(real_fd.get_type(), ft_real);
    auto real_lengths = real_fd.get_real_lengths();
    EXPECT_EQ(real_lengths.first, 4);
    EXPECT_EQ(real_lengths.second, 2);

    // Float field
    field_descriptor float_fd = create_float_field("float_field", 5, 1);
    EXPECT_EQ(float_fd.name, "float_field");
    EXPECT_EQ(float_fd.get_type(), ft_float);
    // For float, get_real_lengths returns mantissa and exponent
    auto float_lengths = float_fd.get_real_lengths();
    EXPECT_EQ(float_lengths.first, 5);
    EXPECT_EQ(float_lengths.second, 1);
}

TEST(FieldDescriptorTests, SettersAndGetters) {
    field_descriptor fd;
    fd.name = "test";

    fd.set_as_integer(10);
    EXPECT_EQ(fd.get_type(), ft_integer);
    EXPECT_EQ(fd.get_integer_width(), 10);

    fd.set_as_bool();
    EXPECT_EQ(fd.get_type(), ft_bool);

    fd.set_as_text(20);
    EXPECT_EQ(fd.get_type(), ft_text);
    EXPECT_EQ(fd.get_text_max_length(), 20);

    fd.set_as_binary(15);
    EXPECT_EQ(fd.get_type(), ft_binary);
    EXPECT_EQ(fd.get_binary_max_length(), 15);

    fd.set_as_date(4);
    EXPECT_EQ(fd.get_type(), ft_date);
    EXPECT_EQ(fd.get_date_fraction_digits(), 4);

    fd.set_as_real(7, 3);
    EXPECT_EQ(fd.get_type(), ft_real);
    auto real_lengths = fd.get_real_lengths();
    EXPECT_EQ(real_lengths.first, 7);
    EXPECT_EQ(real_lengths.second, 3);

    fd.set_as_float(6, 2);
    EXPECT_EQ(fd.get_type(), ft_float);
    auto float_lengths = fd.get_real_lengths();
    EXPECT_EQ(float_lengths.first, 6);
    EXPECT_EQ(float_lengths.second, 2);
}

TEST(FieldDescriptorTests, TypeName) {
    field_descriptor fd;
    fd.set_as_integer();
    EXPECT_EQ(fd.get_type_name(), "integer");

    fd.set_as_bool();
    EXPECT_EQ(fd.get_type_name(), "bool");

    fd.set_as_text();
    EXPECT_EQ(fd.get_type_name(), "text");

    fd.set_as_binary();
    EXPECT_EQ(fd.get_type_name(), "binary");

    fd.set_as_date();
    EXPECT_EQ(fd.get_type_name(), "date");

    fd.set_as_real();
    EXPECT_EQ(fd.get_type_name(), "real");

    fd.set_as_float();
    EXPECT_EQ(fd.get_type_name(), "float");
}
TEST(FieldDescriptorTests, ReadWriteSpan) {
    // Integer field
    field_descriptor int_fd = create_integer_field("int_field", 8);
    std::vector<uint8_t> buffer(field_descriptor::get_size(), 0);
    int_fd.write_to_span({ buffer.data(), buffer.size() });
    field_descriptor int_fd2;
    int_fd2.read_from_span({ buffer.data(), buffer.size() });
    EXPECT_EQ(int_fd2.name, int_fd.name);
    EXPECT_EQ(int_fd2.get_type(), int_fd.get_type());
    EXPECT_EQ(int_fd2.get_integer_width(), int_fd.get_integer_width());

    // Text field
    field_descriptor text_fd = create_text_field("text_field", 16);
    std::vector<uint8_t> buffer2(field_descriptor::get_size(), 0);
    text_fd.write_to_span({ buffer2.data(), buffer2.size() });
    field_descriptor text_fd2;
    text_fd2.read_from_span({ buffer2.data(), buffer2.size() });
    EXPECT_EQ(text_fd2.name, text_fd.name);
    EXPECT_EQ(text_fd2.get_type(), text_fd.get_type());
    EXPECT_EQ(text_fd2.get_text_max_length(), text_fd.get_text_max_length());

    // Real field
    field_descriptor real_fd = create_real_field("real_field", 4, 2);
    std::vector<uint8_t> buffer3(field_descriptor::get_size(), 0);
    real_fd.write_to_span({ buffer3.data(), buffer3.size() });
    field_descriptor real_fd2;
    real_fd2.read_from_span({ buffer3.data(), buffer3.size() });
    EXPECT_EQ(real_fd2.name, real_fd.name);
    EXPECT_EQ(real_fd2.get_type(), real_fd.get_type());
    auto real_lengths = real_fd.get_real_lengths();
    auto real_lengths2 = real_fd2.get_real_lengths();
    EXPECT_EQ(real_lengths2.first, real_lengths.first);
    EXPECT_EQ(real_lengths2.second, real_lengths.second);
}
