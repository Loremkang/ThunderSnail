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

void HashTableForNewLinkReset(HashTableForNewLinkT *hashTable) {
    hashTable->count = 0;
    if (hashTable->capacity > 0) {
        memset(hashTable->entries, 0, sizeof(HashTableForNewLinkEntryT) * hashTable->capacity);
    }
}


// automatically reset while expand
void HashTableForNewLinkExpand(HashTableForNewLinkT *hashTable, int capacity) {
    ValidValueCheck(capacity > 0);
    if (hashTable->capacity >= capacity) {
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
static inline int TryGetIdAtPosition(HashTableForNewLinkT *hashTable, int slotId, TupleIdOrMaxLinkAddrT key) {
    if (hashTable->entries[slotId].value == 0) { // empty slot. insert
        hashTable->entries[slotId].key = key;
        hashTable->entries[slotId].value = ++hashTable->count; // id starting from 1
        return hashTable->entries[slotId].value;
    } else if (TupleIdOrMaxLinkAddrEqual(hashTable->entries[slotId].key, key)) { // match
        return hashTable->entries[slotId].value;
    } else { // mismatch
        return 0;
    }
}

static inline uint64_t Hash(TupleIdOrMaxLinkAddrT key) {
    uint64_t hash_result = 0;
    if (key.type == TupleId) {
        hash_result = hash64((uint64_t)key.value.tupleId.tableId) ^ hash64((uint64_t)key.value.tupleId.tupleAddr);
    } else if (key.type == MaxLinkAddr) {
        hash_result = hash64(RemotePtrToI64(key.value.maxLinkAddr.rPtr));
    } else {
        ValueOverflowCheck(0);
    }
    return hash_result;
}

int HashTableForNewlinkGetId(HashTableForNewLinkT *hashTable, TupleIdOrMaxLinkAddrT key) {
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
}

bool HashTableForNewLinkTest() {
    const int MAXN = 1000;
    HashTableForNewLinkT ht;
    HashTableForNewLinkInit(&ht);
    HashTableForNewLinkExpand(&ht, 1.5 * MAXN); // very dense
    TupleIdOrMaxLinkAddrT keys[MAXN];
    for (int i = 0; i < MAXN; i ++) {
        if (i & 1) {
            keys[i].type = TupleId;
            keys[i].value.tupleId.tableId = hash32(i);
            keys[i].value.tupleId.tupleAddr = hash64(i);
        } else {
            keys[i].type = MaxLinkAddr;
            keys[i].value.maxLinkAddr.rPtr = RemotePtrFromI64(hash64(i));
        }
        int id = HashTableForNewlinkGetId(&ht, keys[i]);
        printf("i=%d\tid=%d\n", i, id);
        assert(id == i + 1);
    }
    for (int i = 7; i != 0; i = (i + 7) % MAXN) {
        int id = HashTableForNewlinkGetId(&ht, keys[i]);
        printf("i=%d\tid=%d\n", i, id);
        assert(id == i + 1);
    }
    return true;
}