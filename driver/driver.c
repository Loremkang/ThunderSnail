#include "driver.h"
#include "fake_interface.h"
#include "safety_check_macro.h"
#include "cpu_buffer_builder.h"
#include "protocol.h"
#include "shared_constants.h"
#include "disjoint_set.h"


#define PRIMARY_INDEX_MAX_NUM 16

static inline uint32_t _hash_function(uint8_t *buf, uint32_t len)
{
    uint32_t hash = 3917;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
    return hash;
}

size_t RunGetOrInsert(IOManagerT *ioManager, struct dpu_set_t* set, int batchSize, int TableId, TupleIdT *tupleIds,
                         TupleIdT *resultTupleIds,
                         HashTableQueryReplyT *resultCounterpart) {
    static bool hashTableInitialized[PRIMARY_INDEX_MAX_NUM] = {false};
    int hashTableCount = CatalogHashTableCountGet(TableId);
    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        int hashTableId = CatalogEdgeIdGet(TableId, hashTableIdx);
        ArrayOverflowCheck(hashTableId < PRIMARY_INDEX_MAX_NUM);
        if (!hashTableInitialized[hashTableId]) {
            hashTableInitialized[hashTableId] = true;
            SendCreateIndexReq(*set, hashTableId);
        }
    }

    size_t resultCount = 0;

    IOManagerStartBufferBuilder(ioManager);
    IOManagerBeginBlock(ioManager, GET_OR_INSERT_REQ);

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
            memcpy(req->ptr, key, keyLength);
            // append one task for each dpu
            IOManagerAppendTask(ioManager, dpuIdx, (Task *)req);
            req->taskIdx = resultCount;
            resultTupleIds[resultCount] = tupleIds[i];
        }
        free(key);
        free(req);
    }

    IOManagerEndBlock(ioManager);
    IOManagerFinish(ioManager);

    IOManagerSendExecReceive(ioManager);

#ifdef DEBUG
    bool used[MAXSIZE_HASH_TABLE_QUERY_BATCH] = {false};
#endif

    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator = BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                GetOrInsertResp *resp = (GetOrInsertResp *)task;
                resultCounterpart[resp->taskIdx] = resp->tupleIdOrMaxLinkAddr;

                ValidValueCheck(resp->base.taskType == GET_OR_INSERT_RESP);
                #ifdef DEBUG
                used[resp->taskIdx] = true;
                #endif
            }
        }
    }

#ifdef DEBUG
    for (int i = 0; i < resultCount; i++) {
        ValidValueCheck(used[i]);
    }
#endif

    return resultCount;
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

// called in system startup
void DriverInit(DriverT *driver) {

    DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &driver->dpu_set));
    DPU_ASSERT(dpu_load(&driver->dpu_set, DPU_BINARY, NULL));
    SendSetDpuIdReq(driver->set);

    HashTableForNewLinkInit(&driver->ht);
    VariableLengthStructBufferInit(&driver->buf);

    IOManagerInit(driver->ioManager, &driver->set, GlobalIOBuffers, GlobalOffsetsBuffer,
                GlobalVarlenBlockOffsetBuffer, GlobalIOBuffers);
}

void DriverBatchInsertTuple(DriverT *driver, int batchSize, TupleIdT *tupleIds) {
    
    BatchInsertValidCheck(batchSize, tupleIds);

    int tableId = tupleIds[0].tableId;
    int HashTableCount = CatalogHashTableCountGet(tableId);

    size_t resultCount =
        RunGetOrInsert(&driver->ioManager, &driver->dpu_set, batchSize, tableId, tupleIds,
                       driver->resultTupleIds, driver->resultCounterpart);

    GetOrInsertResultToNewlink(resultCount, driver->resultTupleIds, driver->resultCounterpart,
                               &driver->ht, &driver->buf);
}

void DriverFree(DriverT *driver) {
    HashTableForNewLinkFree(&driver->ht);
    VariableLengthStructBufferFree(&driver->buf);
    DPU_ASSERT(dpu_free(driver->dpu_set));
}