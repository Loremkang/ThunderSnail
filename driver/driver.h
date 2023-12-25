#ifndef DRIVER_H
#define DRIVER_H

#include <stdint.h>
#include "common_base_struct.h"
#include "io_manager.h"
#include "hash_table_for_newlink.h"
#include "disjoint_set.h"
#include "variable_length_struct_buffer.h"
#include "shared_constants.h"
#include "requests.h"
#include "newlink.h"
#include "newlink_to_maxlink.h"
#include "catalog.h"

// long lasting structures
typedef struct DriverT {
    
    struct dpu_set_t dpu_set;
    IOManagerT ioManager;

    // Init:
    CatalogT catalog;

    // Stage 1: GetOrInsert Result
    TupleIdT resultTupleIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    HashTableQueryReplyT resultCounterpart[MAXSIZE_HASH_TABLE_QUERY_BATCH];

    // Used In Stage 2: GetOrInsert Result -> NewLink
    HashTableForNewLinkT ht;

    // Stage 2: NewLink Result
    VariableLengthStructBufferT newLinkBuffer;

    // Used In Stage 3: NewLink Result -> MaxLink
    int maxLinkSize[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    int largestMaxLinkPos[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    int largestMaxLinkSize[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    int idx[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    MaxLinkAddrT newMaxLinkAddrs[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    NewLinkMergerT merger;
    VariableLengthStructBufferT preMaxLinkBuffer;

    // Used In Stage 4: Insert MaxLink
    int validMaxLinkCount[NUM_DPU];
    int totalValidMaxLinkCount;
    // VariableLengthStructBufferT validResultBuffer;
} DriverT;

typedef struct KeyT {
    uint8_t* buf;
    size_t size;
} KeyT;

// uint32_t DriverBatchInsertTuple(DriverT *driver, int batchSize, TupleIdT *tupleIds);
uint32_t DriverBatchInsertTupleWithKeys(DriverT *driver, TupleIdT *tupleIds, int batchSize, KeyT* keys);
void DriverInit(DriverT *driver);
void DriverFree(DriverT *driver);

#endif