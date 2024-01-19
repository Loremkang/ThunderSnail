#include "path_config.h"
#include <stdint.h>

uint32_t edgeIndexArr[EDGE_INDEX_LEN] = {
    1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};

uint32_t tableIndexArr[TABLE_INDEX_LEN] = {
    1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};

uint32_t pathCodes[PATH_CODES_LEN] = {
    0b1,         0b11,         0b101,         0b1101,
    0b11101,     0b101101,     0b1000101,     0b10000101,
    0b100000101, 0b1000000101, 0b10000000001, 0b100000000001};