#include "common_base_struct/common_base_struct.h"

typedef struct {
    int tupleIDCount;
    int maxLinkAddrCount;
    uint8_t buffer[]; // [[Tuple IDs], [MaxLink Addrs]]
} NewLinkT;

TupleIdT* NewLinkGetTupleIDs(NewLinkT* newLink);
MaxLinkAddrT* NewLinkGetMaxLinkAddrs(NewLinkT* newLink);
int NewLinkGetSize(int tupleIdCount, int maxLinkAddrCount);
void NewLinkPrint(NewLinkT* newLink);
void NewLinkTest();