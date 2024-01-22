#pragma once
#ifndef GQL_ENCODE_H
#define GQL_ENCODE_H
#include <stdint.h>

#define EDGE_INDEX_LEN 30
extern uint32_t edgeIndexArr[EDGE_INDEX_LEN];

#define TABLE_INDEX_LEN 30
extern uint32_t tableIndexArr[TABLE_INDEX_LEN];

#define PATH_CODES_LEN 12
extern uint32_t pathCodes[PATH_CODES_LEN];
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