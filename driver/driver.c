#include "driver.h"
#include "fake_interface.h"
#include "safety_check_macro.h"
#include "cpu_buffer_builder.h"
#include "protocol.h"
#include "shared_constants.h"
#include "io_manager.h"

#define PRIMARY_INDEX_MAX_NUM 16

static inline uint32_t _hash_function(uint8_t *buf, uint32_t len)
{
    uint32_t hash = 3917;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
    return hash;
}

void RunGetOrInsert(IOManagerT ioManager, struct dpu_set_t* set, int batchSize, int TableId, TupleIdT *tupleIds,
                         TupleIdT *resultTupleIds,
                         HashTableQueryReplyT *resultCounterpart) {
    static bool hashTableInitialized[PRIMARY_INDEX_MAX_NUM] = {false};
    int hashTableCount = CatalogHashTableCountGet(TableId);
    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        int hashTableId = CatalogEdgeIdGet(TableId, hashTableIdx);
        ArrayOverflowCheck(hashTableId < PRIMARY_INDEX_MAX_NUM);
        if (!hashTableInitialized[hashTableId]) {
            hashTableInitialized[hashTableId] = true;
            SendCreateIndexReq(hashTableId);
        }
    }

    IOManagerInit(&ioManager, &set, GlobalIOBuffers, GlobalOffsetsBuffer,
                  GlobalVarlenBlockOffsetBuffer, GlobalIOBuffers);
    IOManagerBeginBlock(&ioManager, GET_OR_INSERT_REQ);

    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        ValidValueCheck(batchSize > 0);
        int hashTableId = CatalogEdgeIdGet(TableId, hashTableIdx);
        int keyLength = CatalogHashTableKeyLengthGet(hashTableId);
        uint8_t *key = malloc(keyLength);

        size_t taskSize = ROUND_UP_TO_8(keyLength + sizeof(GetOrInsertReq));
        GetOrInsertReq *req = malloc(taskSize);
        memset(req, 0, taskSize);
        req->base = (Task){.taskType = GET_OR_INSERT_REQ};
        req->len = keyLength;
        req->hashTableId = hashTableId;

        for (int i = 0; i < batchSize; i++) {
            CatalogHashTableKeyGet(tupleIds[i], hashTableIdx, key);
            int dpuIdx = _hash_function(key, keyLength) % NUM_DPU;
            req->tid = tupleIds[i];
            memcpy(req->ptr, keys[i].data, keys[i].len);
            // append one task for each dpu
            IOManagerAppendTask(&ioManager, dpuIdx, (Task *)req);
        }
        free(key);
        free(req);
    }

    IOManagerEndBlock(&ioManager);
    IOManagerFinish(&ioManager);

    IOManagerSendExecReceive(&ioManager);

}

void BatchInsertValidCheck(int batchSize, TupleIdT *tupleIds) {
    for (int i = 1; i < batchSize; i++) {
        ValidValueCheck(tupleIds[i].tableId == tupleIds[i - 1].tableId);
    }
    for (int i = 0; i < batchSize; i++) {
        ValidValueCheck(tupleIds[i].tableId >= 1 && tupleIds[i].tableId <= 5);
        ValidValueCheck(tupleIds[i].tupleAddr >= 0 &&
                        tupleIds[i].tupleAddr < (1 << 24));
    }
}

void BatchInsertTuple(int batchSize, TupleIdT *tupleIds, struct dpu_set_t* set, IOManagerT *ioManager) {
    BatchInsertValidCheck(batchSize, tupleIds);

    TupleIdT resultTupleIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    HashTableQueryReplyT resultCounterpart[MAXSIZE_HASH_TABLE_QUERY_BATCH];

    int tableId = tupleIds[0].tableId;
    int HashTableCount = CatalogHashTableCountGet(tableId);

    RunGetOrInsert(ioManager, set, batchSize, tableId, tupleIds, resultTupleIds,
                        resultCounterpart);
}