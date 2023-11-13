#include "../common_base_struct/common_base_struct.h"

typedef struct {
    int tupleIDCount;
    int maxLinkAddrCount;
    uint8_t buffer[]; // [[Tuple IDs], [MaxLink Addrs]]
} NewLinkT;