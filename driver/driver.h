#ifndef DRIVER_H
#define DRIVER_H

#include <stdint.h>
#include "common_base_struct.h"
#include "io_manager.h"
#include "hash_table_for_newlink.h"
#include "variable_length_struct_buffer.h"
#include "requests.h"

// long lasting structures
typedef struct DriverT {
    
    struct dpu_set_t dpu_set;
    IOManagerT ioManager;

    // Stage 1: GetOrInsert Result
    TupleIdT resultTupleIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    HashTableQueryReplyT resultCounterpart[MAXSIZE_HASH_TABLE_QUERY_BATCH];

    // Used In Stage 2: GetOrInsert Result -> NewLink
    HashTableForNewLinkT ht;

    // Stage 2: NewLink Result
    VariableLengthStructBufferT buf;

} DriverT;

void DriverBatchInsertTuple(DriverT *driver, int batchSize, TupleIdT *tupleIds);
void DriverInit(DriverT *driver);

#endif