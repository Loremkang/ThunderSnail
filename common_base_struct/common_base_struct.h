#pragma once
#ifndef COMMON_BASE_STRUCT_H
#define COMMON_BASE_STRUCT_H

#include <stdint.h>
#include <stdbool.h>
#include "../safety_check_macro.h"

// Pointers
typedef struct {
    uint32_t dpuId;
    uint32_t dpuAddr;
} RemotePtrT;

void RemotePtrPrint(RemotePtrT rPtr);

#define INVALID_REMOTEPTR ((RemotePtrT){.dpuId = UINT32_MAX, .dpuAddr = UINT32_MAX})

uint64_t RemotePtrToI64(RemotePtrT rPtr);
RemotePtrT RemotePtrFromI64(uint64_t i64);
bool RemotePtrInvalid(RemotePtrT rPtr);

typedef struct {
    RemotePtrT rPtr;
} HashAddrT;

typedef struct {
    RemotePtrT rPtr;
} MaxLinkAddrT;

void MaxLinkAddrPrint(MaxLinkAddrT maxLinkAddr);

// Base Elements
typedef struct {
    int tableId;
    int64_t tupleAddr;
} TupleIdT;

void TupleIdPrint(TupleIdT tupleId);

// used as reply value of "HashTableGetOrInsert"
typedef enum {
    TupleId = 0,
    MaxLinkAddr = 1,
    HashAddr = 2
} HashTableQueryReplyTypeT;

typedef union {
    TupleIdT tupleId;
    MaxLinkAddrT maxLinkAddr;
    HashAddrT hashAddr;
} HashTableQueryReplyValueT;

typedef struct HashTableQueryReplyT{
    HashTableQueryReplyTypeT type;
    HashTableQueryReplyValueT value;
} HashTableQueryReplyT;

bool HashTableQueryReplyEqual(HashTableQueryReplyT a, HashTableQueryReplyT b);

typedef struct {
    int tupleIDCount;
    int hashAddrCount;
    uint8_t buffer[]; // [[Tuple IDs], [Hash Addrs]]
} MaxLink;

inline TupleIdT* GetKthTupleIDFromMaxLink(MaxLink* maxLink, int i);
inline HashAddrT* GetKthHashAddrFromMaxLink(MaxLink* maxLink, int i);
int GetMaxLinkSize(int tupleIdCount, int hashAddrCount);
int BuildMaxLink(int tupleIdCount, int hashAddrCount, TupleIdT* tupleIds, HashAddrT* hashAddrs);

#endif
