#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <mram.h>
#include <alloc.h>
#include <string.h>
#include "mram_space.h"
#include "mutex.h"
#include "protocol.h"
#include "path_config.h"
#include "maxlink.h"
#include "global_mutex.h"
#include "common_base_struct.h"

// A entry is something like 
// [tid_coaunt, hash_count, T(1,1), NULL, NULL, NULL, H(1,1), NULL, NULL]
// their sizes are the following
__mram_ptr void* mram_stack_ptr = (__mram_ptr void*)STACK_BOTTOM_ADDR;

// a global pointer for memory management
// it will just bump up when a new entry is appended.
__mram_ptr void* new_max_link_entry_ptr = (__mram_ptr void*)MAX_LINK_ENTRY_START_ADDR;

__host int counter;
MUTEX_INIT(counter_mutex);

MUTEX_INIT(heap_mutex);

void CounterInc(int value) {
    mutex_lock(counter_mutex);
    counter += value;
    mutex_unlock(counter_mutex);
}

bool IsNullTuple(TupleIdT* tid) {
    // Since the address can be 0, UINT64_MAX is used to denote 
    // null value
    return tid->tupleAddr == UINT64_MAX;
}

bool IsNullHash(HashAddrT* ha) {
    return RemotePtrInvalid(ha->rPtr);
    // return ha->rPtr.dpuAddr == UINT32_MAX;
} 

uint32_t GetTableIdIndex(int tid) {
    for (int i = 0; i < TABLE_INDEX_LEN; i++) {
        if (tableIndexArr[i] == tid) {
            return i;
        }
    }
    printf("assert filed in file: %s, line: %d, input: %d\n", __FILE__, __LINE__, tid);
    assert(false);
    // for error case
    return UINT32_MAX;
}

uint32_t GetEdgeIdIndex(int tid) {
    for (int i = 0; i < EDGE_INDEX_LEN; i++) {
        if (edgeIndexArr[i] == tid) {
            return i;
        }
    }
    printf("assert filed in file: %s, line: %d, input: %d\n", __FILE__, __LINE__, tid);
    assert(false);
    // for error case
    return UINT32_MAX;
}

uint32_t EncodeMaxLinkEntry(MaxLinkEntryT* link) {
    uint32_t code = 0;
    for (int i = 0; i < TABLE_INDEX_LEN; i ++) {
        if (!IsNullTuple(link->tupleIds)) {
            code |= 1 << i;
        }
    }
    return code;
}

bool CheckValidMaxLink(MaxLinkEntryT* link) {
    uint32_t code = EncodeMaxLinkEntry(link);
    for (int i = 0; i < PATH_CODES_LEN; i++) {
        if (code & pathCodes[i] == pathCodes[i]) {
            return true;
        }
    }
    return false;
}

__mram_ptr MaxLinkEntryT* NewMaxLinkEntry(MaxLinkT* ml) {
    // allocate memory on wram
    __dma_aligned MaxLinkEntryT res;
    // expand MaxLinkT to MaxLinkEntryT
    // write Tuple
    
    // initialize to UINT32/64_MAX
    memset(&res, 0xff, sizeof(res));

    res.tupleIDCount = ml->tupleIDCount;
    res.hashAddrCount = ml->hashAddrCount;

    TupleIdT* tuple_buf = MaxLinkGetTupleIDs(ml);
    for (int i = 0; i < ml->tupleIDCount; i++) {
        int idx = GetTableIdIndex(tuple_buf[i].tableId);
        // TupleIdPrint(res.tupleIds[idx]);
        res.tupleIds[idx] = tuple_buf[i];
        // printf("idx = %d", idx);
        // TupleIdPrint(tuple_buf[i]);
    }
    // write hash
    HashAddrT* hash_buf = MaxLinkGetHashAddrs(ml);
    for (int i = 0; i < ml->hashAddrCount; i++) {
        int idx = GetEdgeIdIndex(hash_buf[i].edgeId);
        // printf("idx = %d", idx);
        res.hashAddrs[idx] = hash_buf[i];
        // HashAddrPrint(hash_buf[i]);
    }
    printf("valid = %d\n", (int)CheckValidMaxLink(&res));
    if (CheckValidMaxLink(&res)) {
        CounterInc(1);
    }
    

    mutex_lock(heap_mutex);
    __mram_ptr MaxLinkEntryT* maxLinkEntryPtr = (__mram_ptr MaxLinkEntryT*)new_max_link_entry_ptr;
    // add new_max_link_entry_ptr for new allocation
    new_max_link_entry_ptr += MAX_LINK_ENTRY_SIZE;
    mutex_unlock(heap_mutex);
    // // copy it to mram
    mram_write_unaligned(&res, maxLinkEntryPtr, MAX_LINK_ENTRY_SIZE);
    return maxLinkEntryPtr;
}

void MergeMaxLink(__mram_ptr MaxLinkEntryT* dst, MaxLinkT* src) {
    // allocate wram
    __dma_aligned MaxLinkEntryT res;
    // copy target to wram
    mram_read(dst, &res, MAX_LINK_ENTRY_SIZE);
    bool alreadyValid = CheckValidMaxLink(&res);

    TupleIdT* tuple_buf = MaxLinkGetTupleIDs(src);
    // merge table id
    for (int i = 0; i < src->tupleIDCount; i++) {
        int idx = GetTableIdIndex(tuple_buf[i].tableId);
        ValidValueCheck(IsNullTuple(res.tupleIds + idx));
        ValidValueCheck(!IsNullTuple(tuple_buf + i));
        res.tupleIds[idx] = tuple_buf[i];
    }
    // no duplicated table id, add directly
    res.tupleIDCount+=src->tupleIDCount;

    // merge hash id
    // hash id can be the same, so hash address count should be added if 
    // it is not in hash 
    HashAddrT* hash_buf = MaxLinkGetHashAddrs(src);
    for (int i = 0; i < src->hashAddrCount; i++) {
        int idx = GetEdgeIdIndex(hash_buf[i].edgeId);
        if (IsNullHash(&res.hashAddrs[idx])) {
            res.hashAddrs[idx] = hash_buf[i];
            res.hashAddrCount++;
        } else {
            ValidValueCheck(HashAddrEqual(res.hashAddrs[idx], hash_buf[i]));
        }
    }
    if (!alreadyValid && CheckValidMaxLink(&res)) {
        CounterInc(1);
    }
    // write res to target mram
    mram_write_unaligned(&res, dst, MAX_LINK_ENTRY_SIZE);
}

void MergeMaxLinkEntry(MaxLinkEntryT* target, MaxLinkEntryT* source) {
    for(int i = 0; i < target->tupleIDCount; i++) {
        target->tupleIds[i] = source->tupleIds[i];
    }
    for (int i = 0; i< target->hashAddrCount; i++) {
        target->hashAddrs[i] = source->hashAddrs[i];
    }
}

void RetriveMaxLink(__mram_ptr MaxLinkEntryT* src, MaxLinkT* res) {
    // allocate stack memory
    __dma_aligned MaxLinkEntryT cpy;
    // move src to cpy
    mram_read(src, &cpy, MAX_LINK_ENTRY_SIZE);
    
    res->tupleIDCount = cpy.tupleIDCount;
    res->hashAddrCount = cpy.hashAddrCount;

    // write Tuple
    TupleIdT* tids = MaxLinkGetTupleIDs(res);
    for(int i = 0; i < TABLE_INDEX_LEN; i++) {
        if(!IsNullTuple(&cpy.tupleIds[i])) {
            *tids = cpy.tupleIds[i];
            tids++;
        }
    }

    // write hash. Here tids should be equal to hids
    HashAddrT* hids = MaxLinkGetHashAddrs(res);
    for (int i = 0; i < EDGE_INDEX_LEN; i++) {
        if(!IsNullHash(&cpy.hashAddrs[i])) {
            *hids = cpy.hashAddrs[i];
            hids++;
        }
    }

    if (CheckValidMaxLink(&cpy)) {
        CounterInc(-1);
    }
}

// size after compaction
uint32_t GetMaxLinkSize(__mram_ptr MaxLinkEntryT* src) {
    __dma_aligned MaxLinkEntryT entry;
    mram_read(src, &entry, sizeof(entry));
    uint32_t result = sizeof(MaxLinkT);
    // printf("Tuple: count = %d, size = %d\n", entry.tupleIDCount, TUPLE_ID_SIZE);
    // printf("Hash: count = %d, size = %d\n", entry.hashAddrCount, HASH_ADDR_SIZE);
    result += entry.tupleIDCount * TUPLE_ID_SIZE;
    result += entry.hashAddrCount * HASH_ADDR_SIZE;
    return result;
}