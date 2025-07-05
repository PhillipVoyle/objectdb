#include "../include/core.hpp"

int compare_span(const std::span<uint8_t>& a, const std::span<uint8_t>& b)
{
    auto ita = a.begin();
    auto itb = b.begin();

    for (;;)
    {
        if (ita == a.end())
        {
            if (itb == b.end())
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
        else if (itb == b.end())
        {
            return 1;
        }
        else if (*ita < *itb)
        {
            return -1;
        }
        else if (*itb < *ita)
        {
            return 1;
        }
        else// if (*itb == *ita)
        {
            ita++;
            itb++;
        }
    }
}