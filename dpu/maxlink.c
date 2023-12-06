#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#include "mram_space.h"
#include "mutex.h"
#include "protocol.h"
#include "path_config.h"
#include "vmutex.h"
#include "maxlink.h"


// A entry is something like 
// [tid_coaunt, hash_count, T(1,1), NULL, NULL, NULL, H(1,1), NULL, NULL]
// their sizes are the following
VMUTEX_INIT(mutexes, 64, 32)
__mram_ptr void* mram_stack_ptr = (__mram_ptr void*)STACK_BOTTOM_ADDR;

// a global pointer for memory management
// it will just bump up when a new entry is appended.
__mram_ptr void* new_max_link_entry_ptr = (__mram_ptr void*)MAX_LINK_ENTRY_START_ADDR;

int counter = 0;
MUTEX_INIT(counter_mutex);

bool IsNullTuple(TupleIdT* tid) {
    // Since the address can be 0, UINT64_MAX is used to denote 
    // null value
    return tid->tupleAddr == UINT64_MAX;
}

bool IsNullHash(HashAddrT* ha) {
    return ha->rPtr.dpuAddr == UINT32_MAX;
} 

// 
uint32_t GetIdIndex(int tid) {
    for (int i = 0; i < TABLE_INDEX_LEN; i++) {
        if (tableIndexArr[i] == tid) {
            return i;
        }
    }
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
    __mram_ptr MaxLinkEntryT* max_link_entry = (__mram_ptr MaxLinkEntryT*)new_max_link_entry_ptr;
    // add new_max_link_entry_ptr for new allocation
    new_max_link_entry_ptr += MAX_LINK_ENTRY_SIZE;

    // write Tuple
    TupleIdT* tuple_buf = ml->buffer;
    for (int i = 0; i < ml->tupleIDCount; i++) {
        int idx = GetIdIndex(tuple_buf[i].tableId);
        max_link_entry->tupleIds[idx] = tuple_buf[i];
    }
    // write hash
    HashAddrT* hash_buf = ml->buffer + (TABLE_INDEX_LEN * TUPLE_ID_SIZE);
    for (int i = 0; i < ml->hashAddrCount; i++) {
        int idx = GetIdIndex(hash_buf[i].rPtr.dpuId);
        max_link_entry->hashAddrs[idx] = hash_buf[i];
        hash_buf++;
    }
    max_link_entry->tableIDCount = ml->tupleIDCount;
    max_link_entry->hashAddrCount = ml->hashAddrCount;

    return max_link_entry;
}

// check error
void MergeMaxLink(MaxLinkEntryT* target, MaxLinkT* source) {
    TupleIdT* tuple_buf = source->buffer;
    // merge table id
    for (int i = 0; i < source->tupleIDCount; i++) {
        int idx = GetIdIndex(tuple_buf[i].tableId);
        assert(IsNullTuple(target->tupleIds + idx) && !IsNullTuple(tuple_buf + i));
        target->tupleIds[idx] = tuple_buf[i];
    }
    // no duplicated table id, add directly
    target->tableIDCount+=source->tupleIDCount;

    // merge hash id
    // hash id can be the same, so hash address count should be added if 
    // it is not in hash 
    HashAddrT* hash_buf = source->buffer + (source->tupleIDCount * TUPLE_ID_SIZE);
    for (int i = 0; i < source->hashAddrCount; i++) {
        int idx = GetIdIndex(hash_buf[i].rPtr.dpuId);
        assert(!IsNullHash(&target->hashAddrs[idx]) && !IsNullHash(&hash_buf[i]) ? 
                target->hashAddrs[idx].rPtr.dpuAddr == hash_buf[i].rPtr.dpuAddr &&
                target->hashAddrs[idx].rPtr.dpuId == hash_buf[i].rPtr.dpuId
                : true);
        if(IsNullHash(&target->hashAddrs[idx])) {
            target->hashAddrs[idx] = hash_buf[i];
            target->hashAddrCount++;
        }
    }
}

void MergeMaxLinkEntry(MaxLinkEntryT* target, MaxLinkEntryT* source) {
    for(int i = 0; i < target->tableIDCount; i++) {
        target->tupleIds[i] = source->tupleIds[i];
    }
    for (int i = 0; i< target->hashAddrCount; i++) {
        target->hashAddrs[i] = source->hashAddrs[i];
    }
}

// will return at the end of mram of space
__mram_ptr MaxLinkT* RetriveMaxLink(MaxLinkEntryT* e) {
    // allocate stack memory
    uint32_t size = e->tableIDCount*TUPLE_ID_SIZE 
                  + e->hashAddrCount*HASH_ADDR_SIZE
                  + 2*sizeof(int);

    // alloc stack space
    __mram_ptr MaxLinkT* res = (__mram_ptr MaxLinkT*)(STACK_BOTTOM_ADDR - MAX_LINK_ENTRY_SIZE);
    res->tupleIDCount = e->tableIDCount;
    res->hashAddrCount = e->hashAddrCount;
    __mram_ptr TupleIdT* tids = res->buffer;
    // write Tuple
    for(int i = 0; i < TABLE_INDEX_LEN; i++) {
        if(!IsNullTuple(e->tupleIds + i)) {
            *tids = e->tupleIds[i];
            tids++;
        }
    }

    // write hash. Here tids should be equal to hids
    __mram_ptr HashAddrT* hids = res->buffer + (res->tupleIDCount * TUPLE_ID_SIZE);
    for (int i = 0; i < EDGE_INDEX_LEN; i++) {
        if(!IsNullHash(e->hashAddrs+i)) {
            *hids = e->hashAddrs[i];
            hids++;
        }
    }
    return res;
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