#include "pch.h"
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <unordered_map>
#include <cstring>
#include <iostream>

#include "../include/memory_random_access_file.hpp"
#include "../include/file_object_allocator.hpp"


TEST(free_list_tests, alloc_free_alloc)
{
    memory_random_access_file mraf;

    file_object_allocator_impl foa(mraf, 48);

    filesize_t block_1 = 0;
    filesize_t block_2 = 0;
    filesize_t block_3 = 0;
    filesize_t block_4 = 0;

    foa.allocate_block(block_1);
    foa.allocate_block(block_2);
    foa.allocate_block(block_3);
    foa.allocate_block(block_4);

    EXPECT_EQ(48, block_1);
    EXPECT_EQ(96, block_2);
    EXPECT_EQ(144 , block_3);
    EXPECT_EQ(192, block_4);


    foa.free_block(block_1);
    foa.free_block(block_2);
    foa.free_block(block_3);

    filesize_t block_5 = 0;
    filesize_t block_6 = 0;
    filesize_t block_7 = 0;
    foa.allocate_block(block_5);
    foa.allocate_block(block_6);
    foa.allocate_block(block_7);

    EXPECT_EQ(block_1, block_5);
    EXPECT_EQ(block_2, block_6);
    EXPECT_EQ(block_3, block_7);
}