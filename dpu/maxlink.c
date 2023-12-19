#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <mram.h>
#include <alloc.h>
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

int counter = 0;
MUTEX_INIT(counter_mutex);

MUTEX_INIT(heap_mutex);

bool IsNullTuple(TupleIdT* tid) {
    // Since the address can be 0, UINT64_MAX is used to denote 
    // null value
    return tid->tupleAddr == UINT64_MAX;
}

bool IsNullHash(HashAddrT* ha) {
    return ha->rPtr.dpuAddr == UINT32_MAX;
} 

uint32_t GetIdIndex(int tid) {
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

uint32_t EncodeMaxLink(MaxLinkT* link) {
    uint32_t code = 0;
    TupleIdT* temp = (TupleIdT*)link->buffer;
    for (int i = 0; i < link->tupleIDCount; i++) {
        code |= 1 << GetIdIndex(temp->tableId);
        temp++;
    }
    return code;
}

__mram_ptr MaxLinkEntryT* NewMaxLinkEntry(MaxLinkT* ml) {
    // allocate memory on wram
    MaxLinkEntryT* res = buddy_alloc(MAX_LINK_ENTRY_SIZE);
    // expand MaxLinkT to MaxLinkEntryT
    // write Tuple
    TupleIdT* tuple_buf = ml->buffer;
    for (int i = 0; i < ml->tupleIDCount; i++) {
        int idx = GetIdIndex(tuple_buf[i].tableId);
        res->tupleIds[idx] = tuple_buf[i];
    }
    // write hash
    HashAddrT* hash_buf = ml->buffer + (TABLE_INDEX_LEN * TUPLE_ID_SIZE);
    for (int i = 0; i < ml->hashAddrCount; i++) {
        int idx = GetIdIndex(hash_buf[i].rPtr.dpuId);
        res->hashAddrs[idx] = hash_buf[i];
        hash_buf++;
    }
    res->tupleIDCount = ml->tupleIDCount;
    res->hashAddrCount = ml->hashAddrCount;

    mutex_lock(heap_mutex);
    __mram_ptr MaxLinkEntryT* maxLinkEntryPtr = (__mram_ptr MaxLinkEntryT*)new_max_link_entry_ptr;
    // add new_max_link_entry_ptr for new allocation
    new_max_link_entry_ptr += MAX_LINK_ENTRY_SIZE;
    mutex_unlock(heap_mutex);
    // copy it to mram
    mram_write(res, maxLinkEntryPtr, MAX_LINK_ENTRY_SIZE);
    buddy_free(res);
    return maxLinkEntryPtr;
}

void MergeMaxLink(__mram_ptr MaxLinkEntryT* target, MaxLinkT* source) {
    // allocate wram
    buckets_mutex_lock((int)target >> 3);
    MaxLinkEntryT* res = buddy_alloc(MAX_LINK_ENTRY_SIZE);
    // copy target to wram
    mram_read(target, res, MAX_LINK_ENTRY_SIZE);

    TupleIdT* tuple_buf = source->buffer;
    // merge table id
    for (int i = 0; i < source->tupleIDCount; i++) {
        int idx = GetIdIndex(tuple_buf[i].tableId);
        assert(IsNullTuple(res->tupleIds + idx) && !IsNullTuple(tuple_buf + i));
        res->tupleIds[idx] = tuple_buf[i];
    }
    // no duplicated table id, add directly
    res->tupleIDCount+=source->tupleIDCount;

    // merge hash id
    // hash id can be the same, so hash address count should be added if 
    // it is not in hash 
    HashAddrT* hash_buf = source->buffer + (source->tupleIDCount * TUPLE_ID_SIZE);
    for (int i = 0; i < source->hashAddrCount; i++) {
        int idx = GetIdIndex(hash_buf[i].rPtr.dpuId);
        assert(!IsNullHash(&res->hashAddrs[idx]) && !IsNullHash(&hash_buf[i]) ?
                res->hashAddrs[idx].rPtr.dpuAddr == hash_buf[i].rPtr.dpuAddr &&
                res->hashAddrs[idx].rPtr.dpuId == hash_buf[i].rPtr.dpuId
                : true);
        if(IsNullHash(&res->hashAddrs[idx])) {
            res->hashAddrs[idx] = hash_buf[i];
            res->hashAddrCount++;
        }
    }
    // write res to target mram
    mram_write(res, target, MAX_LINK_ENTRY_SIZE);
    buckets_mutex_unlock((int)target >> 3);
    buddy_free(res);
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
    MaxLinkEntryT* cpy = buddy_alloc(MAX_LINK_ENTRY_SIZE);
    // move src to cpy
    mram_read(src, cpy, MAX_LINK_ENTRY_SIZE);
    
    res->tupleIDCount = cpy->tupleIDCount;
    res->hashAddrCount = cpy->hashAddrCount;

    // write Tuple
    TupleIdT* tids = res->buffer;
    for(int i = 0; i < TABLE_INDEX_LEN; i++) {
        if(!IsNullTuple(&cpy->tupleIds[i])) {
            *tids = cpy->tupleIds[i];
            tids++;
        }
    }

    // write hash. Here tids should be equal to hids
    HashAddrT* hids = res->buffer + (res->tupleIDCount * TUPLE_ID_SIZE);
    for (int i = 0; i < EDGE_INDEX_LEN; i++) {
        if(!IsNullHash(cpy->hashAddrs+i)) {
            *hids = cpy->hashAddrs[i];
            hids++;
        }
    }
}

uint32_t GetMaxLinkSize(__mram_ptr MaxLinkEntryT* src) {
    MaxLinkEntryT* cpy = buddy_alloc(MAX_LINK_ENTRY_SIZE);
    uint32_t result = sizeof(MaxLinkT);
    result += cpy->tupleIDCount * sizeof(TUPLE_ID_SIZE);
    result += cpy->hashAddrCount * sizeof(HASH_ADDR_SIZE);
    buddy_free(cpy);
    return result;
}

void FetchMaxLink(__mram_ptr MaxLinkEntryT* src, MaxLinkT* dst) {
    mram_read(src, dst, MAX_LINK_ENTRY_SIZE);
    MaxLinkEntryT* dst_mirror = (MaxLinkEntryT*)dst;
    // compaction
    for (int i = 0; i < dst->hashAddrCount; i ++) {
        MaxLinkGetHashAddrs(dst)[i] = dst_mirror->hashAddrs[i];
    }
}

bool Check(MaxLinkT* link) {
    uint32_t code = EncodeMaxLink(link);
    for (int i = 0; i < PATH_CODES_LEN; i++) {
        if (code & pathCodes[i] == pathCodes[i]) {
            mutex_lock(counter_mutex);
            counter++;
            mutex_unlock(counter_mutex);
            return true;
        }
    }
    return false;
}