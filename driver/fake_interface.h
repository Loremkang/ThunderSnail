#ifndef DRIVER_FAKE_INTERFACE_H
#define DRIVER_FAKE_INTERFACE_H

#include "safety_check_macro.h"
#include "common_base_struct.h"
#include "string.h"

// assuming T1 --[H1]-- T2 --[H2]-- T3 --[H3]-- T4 --[H4]-- T5
// Tx = [k1 : 8 bytes, k2 : 8 bytes]
// Tuple(Table = i, Addr/Row = j) = [(i << 24) + j, ((i + 1) << 24) + j]

static inline int CatalogHashTableIdGet(int tableId, int hashTableIndex) {
    ValidValueCheck(hashTableIndex >= 0 && hashTableIndex < CatalogHashTableCountGet(tableId));
    if (tableId == 1) {
        return 1;
    }
    return tableId + (hashTableIndex == 0 ? -1 : 0);
}

static inline int CatalogHashTableCountGet(int tableId) {
    if (tableId == 1 || tableId == 5) {
        return 1;
    } else {
        return 2;
    }
}

static inline int CatalogHashTableKeyLengthGet(int hashTableId) {
    return 8;
}

static inline void CatalogHashTableKeyGet(TupleIdT tupleId, int hashTableIndex, uint8_t* buffer) {
    ValidValueCheck(hashTableIndex >= 0 && hashTableIndex < CatalogHashTableCountGet(tableId));
    if (tupleId.tableId == 1) {
        hashTableIndex = 1;
    }

    uint64_t value = ((tupleId.tableId + hashTableIndex) << 24) + tupleId.tupleAddr;
    memcpy(buffer, &value, sizeof(uint64_t));
}

#endif