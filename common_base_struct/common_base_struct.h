#pragma once
#ifndef COMMON_BASE_STRUCT_H
#define COMMON_BASE_STRUCT_H

#include <stdint.h>
#include <stdbool.h>
#include "../safety_check_macro.h"

// align to 8
#define NUM_FIXED_LEN_BLOCK_INPUT 4
#define NUM_VAR_LEN_BLOCK_INPUT 4
#define NUM_FIXED_LEN_BLOCK_OUTPUT 4
#define NUM_VAR_LEN_BLOCK_OUTPUT 4

#define CPU_BUFFER_HEAD_LEN 8 // |epochNumber blockCnt totalSize|
#define DPU_BUFFER_HEAD_LEN 8 // |bufferState blockCnt totalSize|
#define BLOCK_HEAD_LEN sizeof(BlockDescriptorBase)
#define BATCH_SIZE 320
#define NUM_BLOCKS 8

#define BUFFER_LEN 65535
#define ROUND_UP_TO_8(x) (((x)+7) &~7) // to align key len to 8

#define ALIGN_TO( sizeToAlign, PowerOfTwo )                         \
        (((sizeToAlign) + (PowerOfTwo) - 1) & ~((PowerOfTwo) - 1))

#define ALIGN8 __attribute__((aligned(8)))
typedef uint32_t HashTableId;
typedef uint32_t Offset; // The buffer offset

// Pointers
typedef struct {
    uint32_t dpuId;
    uint32_t dpuAddr;
} RemotePtrT;

// internally used only
typedef union {
    RemotePtrT rPtr;
    uint64_t i64;
} RemotePtrConvertT;

#define INVALID_REMOTEPTR ((RemotePtrT){.dpuId = UINT32_MAX, .dpuAddr = UINT32_MAX})

static inline uint64_t RemotePtrToI64(RemotePtrT rPtr) {
    RemotePtrConvertT tmp;
    tmp.rPtr = rPtr;
    return tmp.i64;
}

static inline RemotePtrT RemotePtrFromI64(uint64_t i64) {
    RemotePtrConvertT tmp;
    tmp.i64 = i64;
    return tmp.rPtr;
}

static inline bool RemotePtrInvalid(RemotePtrT rPtr) {
    return RemotePtrToI64(rPtr) == RemotePtrToI64(INVALID_REMOTEPTR);
}

void RemotePtrPrint(RemotePtrT rPtr);

typedef struct {
    RemotePtrT rPtr;
} HashAddrT;

static inline bool HashAddrEqual(HashAddrT a, HashAddrT b) {
    return RemotePtrToI64(a.rPtr) == RemotePtrToI64(b.rPtr);
}

typedef struct {
    RemotePtrT rPtr;
} MaxLinkAddrT;

static inline bool MaxLinkAddrEqual(MaxLinkAddrT a, MaxLinkAddrT b) {
    return RemotePtrToI64(a.rPtr) == RemotePtrToI64(b.rPtr);
}

void MaxLinkAddrPrint(MaxLinkAddrT maxLinkAddr);

// Base Elements
typedef struct {
    int tableId;
    uint32_t padding;
    uint64_t tupleAddr;
} TupleIdT;

static inline bool TupleIdEqual(TupleIdT a, TupleIdT b) {
    return a.tableId == b.tableId && a.tupleAddr == b.tupleAddr;
}

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

static inline bool HashTableQueryReplyEqual(HashTableQueryReplyT a, HashTableQueryReplyT b) {
    if (a.type != b.type) {
        return false;
    }
    switch (a.type) {
        case TupleId:
            return TupleIdEqual(a.value.tupleId, b.value.tupleId);
        case MaxLinkAddr:
            return MaxLinkAddrEqual(a.value.maxLinkAddr, b.value.maxLinkAddr);
        case HashAddr:
            return HashAddrEqual(a.value.hashAddr, b.value.hashAddr);
        default:
            ValueOverflowCheck(0);
            break;
    }
    return false;
}

typedef struct {
    int tupleIDCount;
    int hashAddrCount;
    uint8_t buffer[]; // [[Tuple IDs], [Hash Addrs]]
} MaxLinkT;

static inline TupleIdT* GetTupleIDsFromMaxLink(MaxLinkT* maxLink) {
    return (TupleIdT*)(maxLink->buffer);
}
static inline TupleIdT* GetKthTupleIDFromMaxLink(MaxLinkT* maxLink, int i) {
    ArrayOverflowCheck(i >= 0 && i < maxLink->tupleIDCount);
    return GetTupleIDsFromMaxLink(maxLink) + i;
}
static inline HashAddrT* GetHashAddrsFromMaxLink(MaxLinkT* maxLink) {
    return (HashAddrT*)(maxLink->buffer + sizeof(TupleIdT) * maxLink->tupleIDCount);
}
static inline HashAddrT* GetKthHashAddrFromMaxLink(MaxLinkT* maxLink, int i) {
    ArrayOverflowCheck(i >= 0 && i < maxLink->hashAddrCount);
    return GetHashAddrsFromMaxLink(maxLink) + i;
}

static inline int GetMaxLinkSize(int tupleIdCount, int hashAddrCount) {
    return sizeof(MaxLinkT) + tupleIdCount * sizeof(TupleIdT) + hashAddrCount * sizeof(HashAddrT);
}

static inline int BuildMaxLink(int tupleIdCount, int hashAddrCount, TupleIdT* tupleIds, HashAddrT* hashAddrs) {
    Unimplemented("BuildMaxLink");
    return 0;
}

#endif
