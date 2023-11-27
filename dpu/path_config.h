#ifndef GQL_ENCODE_H
#define GQL_ENCODE_H
#include <stdint.h>

#define EDGE_INDEX_LEN 14
uint32_t edgeIndexArr[EDGE_INDEX_LEN] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

#define TABLE_INDEX_LEN 12
uint32_t tableIndexArr[TABLE_INDEX_LEN] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

#define PATH_CODES_LEN 12
uint32_t pathCodes[PATH_CODES_LEN] = {
    0b1,
    0b11,
    0b101,
    0b1101,
    0b11101,
    0b101101,
    0b1000101,
    0b10000101,
    0b100000101,
    0b1000000101,
    0b10000000001,
    0b100000000001};
#endif
// ["T1"]
// ["T1","T2"]
// ["T1","T3"]
// ["T1","T3","T4"]
// ["T1","T3","T4","T5"]
// ["T1","T3","T4","T6"]
// ["T1","T3","T7"]
// ["T1","T3","T8"]
// ["T1","T3","T9"]
// ["T1","T3","T10"]
// ["T1","T11"]
// ["T1","T12"]