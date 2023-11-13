#pragma once

#include <stdint.h>
#include "../safety_check_macro.h"

// Pointers
typedef struct {
    uint32_t id;
    uint32_t addr;
} RemotePtrT;

typedef struct {
    RemotePtrT rPtr;
} HashAddrT;

typedef struct {
    RemotePtrT rPtr;
} MaxLinkAddrT;

// Base Elements
typedef struct {
    int tableId;
    int64_t tupleAddr;
} TupleIdT;

// used as reply value of "HashTableGetOrInsert"
typedef union {
    TupleIdT tupleId;
    MaxLinkAddrT maxLinkAddr;
} TupleIdOrMaxLinkAddr;

typedef struct {
    int tupleIDCount;
    int hashAddrCount;
    uint8_t buffer[]; // [[Tuple IDs], [Hash Addrs]]
} MaxLink;

inline TupleIdT* GetKthTupleIDFromMaxLink(MaxLink* maxLink, int i);
inline HashAddrT* GetKthHashAddrFromMaxLink(MaxLink* maxLink, int i);
int GetMaxLinkSize(int tupleIdCount, int hashAddrCount);
int BuildMaxLink(int tupleIdCount, int hashAddrCount, TupleIdT* tupleIds, HashAddrT* hashAddrs);