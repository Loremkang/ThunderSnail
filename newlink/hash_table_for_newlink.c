#include "hash_table_for_newlink.h"
#include "hash_function.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// math function

static inline int RoundUpToPower2(int x) {
    ValueOverflowCheck(x < (1 << 30));
    int ret = 1;
    while (ret <= x) {
        ret <<= 1;
    }
    return ret;
}

static inline int LowBit(int x) {
    return x & (-x);
}

static inline int IsPowerOf2(int x) {
    return x == LowBit(x);
}


// hash table functions

void HashTableForNewLinkInit(HashTableForNewLinkT *hashTable) {
    hashTable->capacity = 0;
    hashTable->count = 0;
    hashTable->entries = NULL;
}

void HashTableForNewLinkFree(HashTableForNewLinkT *hashTable) {
    hashTable->capacity = 0;
    hashTable->count = 0;
    if (hashTable->entries != NULL) {
        free(hashTable->entries);
    }
}

void HashTableForNewLinkSoftReset(HashTableForNewLinkT *hashTable) {
    hashTable->count = 0;
    if (hashTable->capacity > 0) {
        memset(hashTable->entries, 0, sizeof(HashTableForNewLinkEntryT) * hashTable->capacity);
    }
}


// automatically reset while expand
void HashTableForNewLinkExpandAndSoftReset(HashTableForNewLinkT *hashTable, int capacity) {
    ValidValueCheck(capacity > 0);
    if (hashTable->capacity >= capacity) {
        HashTableForNewLinkSoftReset(hashTable);
        return;
    }
    if (hashTable->capacity > 0) {
        free(hashTable->entries);
    }
    capacity = RoundUpToPower2(capacity);
    hashTable->capacity = capacity;
    hashTable->count = 0;
    hashTable->entries = calloc(capacity, sizeof(HashTableForNewLinkEntryT));
}

void HashTableForNewLinkShrink(HashTableForNewLinkT *hashTable, int capacity) {
    Unimplemented("HashTableForNewLinkShrink");
}

static inline int NextSlot(HashTableForNewLinkT *hashTable, int x) {
    return (x + 1) & (hashTable->capacity - 1);
}

// return 0 for failure. otherwise return id.
static inline int TryGetIdAtPosition(HashTableForNewLinkT *hashTable, int slotId, HashTableQueryReplyT key) {
    if (hashTable->entries[slotId].value == 0) { // empty slot. insert
        hashTable->entries[slotId].key = key;
        hashTable->entries[slotId].value = ++hashTable->count; // id starting from 1
        return hashTable->entries[slotId].value;
    } else if (HashTableQueryReplyEqual(hashTable->entries[slotId].key, key)) { // match
        return hashTable->entries[slotId].value;
    } else { // mismatch
        return 0;
    }
}

static inline uint64_t Hash(HashTableQueryReplyT key) {
    uint64_t hash_result = hash64_2(key.type);
    switch (key.type) {
        case TupleId:
            hash_result ^= hash64_2((uint64_t)key.value.tupleId.tableId) ^ hash64_2((uint64_t)key.value.tupleId.tupleAddr);
            break;
        case MaxLinkAddr:
            hash_result ^= hash64_2(RemotePtrToI64(key.value.maxLinkAddr.rPtr));
            break;
        case HashAddr:
            hash_result ^= hash64_2(RemotePtrToI64(key.value.hashAddr.rPtr));
            break;
        default:
            ValidValueCheck(0);
    }
    return hash_result;
}

// return unique mapping Id(key): 1, 2, ...
int HashTableForNewlinkGetId(HashTableForNewLinkT *hashTable, HashTableQueryReplyT key) {
    int capacity = hashTable->capacity;
    int count = hashTable->count;
    ValidValueCheck((capacity >= 0 && IsPowerOf2(capacity) && count < capacity));
    int firstSlotId = Hash(key) & (capacity - 1);
    int resultId = TryGetIdAtPosition(hashTable, firstSlotId, key);
    if (resultId != 0) {
        return resultId;
    }
    for (int slotId = NextSlot(hashTable, firstSlotId); slotId != firstSlotId; slotId = NextSlot(hashTable, slotId)) {
        resultId = TryGetIdAtPosition(hashTable, slotId, key);
        if (resultId != 0) {
            return resultId;
        }
    }
    ValidValueCheck(0);
    return 0;
}