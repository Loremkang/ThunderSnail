#include "driver.h"
#include "fake_interface.h"
#include "safety_check_macro.h"
#include "dpu_buffer_builder.h"
#include "protocol.h"
#include "shared_constants.h"

#define PRIMARY_INDEX_MAX_NUM 16

static inline uint32_t _hash_function(uint8_t *buf, uint32_t len)
{
    uint32_t hash = 3917;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
    return hash;
}

bool hashTableInitialized[PRIMARY_INDEX_MAX_NUM] = {false};

void BuildGetOrInsertReqOnHashTableIndex(int batchSize, int TableId,
                                         TupleIdT *tupleIds,
                                         TupleIdT *resultTupleIds,
                                         HashTableQueryReplyT *resultCounterpart) {
    int hashTableCount = CatalogHashTableCountGet(TableId);
    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        int hashTableId = CatalogHashTableIdGet(TableId, hashTableIdx);
        ArrayOverflowCheck(hashTableId < PRIMARY_INDEX_MAX_NUM);
        if (!hashTableInitialized[hashTableId]) {
            hashTableInitialized[hashTableId] = true;
            SendCreateIndexReq(hashTableId);
        }
    }

    uint8_t *buffers[NUM_DPU];
    size_t sizes[NUM_DPU];
    BufferBuilder builders[NUM_DPU];
    CpuToDpuBufferDescriptor bufferDescs[NUM_DPU];
    for (int i = 0; i < NUM_DPU; i++) {
        memset(&bufferDescs[i], 0, sizeof(CpuToDpuBufferDescriptor));
        bufferDescs[i] =
            (CpuToDpuBufferDescriptor){.header = {
                                           .epochNumber = GetEpochNumber(),
                                       }};
    }

    for (int i = 0; i < NUM_DPU; i++) {
        BufferBuilderInit(&builders[i], &bufferDescs[i]);
        BufferBuilderBeginBlock(&builders[i], GET_OR_INSERT_REQ);
    }

    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        ValidValueCheck(batchSize > 0);
        int hashTableId = CatalogHashTableIdGet(TableId, hashTableIdx);
        int keyLength = CatalogHashTableKeyLengthGet(hashTableId)
        uint8_t *key = malloc(keyLength);

        size_t taskSize =
                        ROUND_UP_TO_8(keyLength + sizeof(GetOrInsertReq));
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
            BufferBuilderAppendTask(&builders[dpuIdx], (Task *)req);
        }
        free(key);
    }
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

void BatchInsertTuple(int batchSize, TupleIdT *tupleIds) {
    BatchInsertValidCheck(batchSize, tupleIds);

    TupleIdT resultTupleIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    HashTableQueryReplyT resultCounterpart[MAXSIZE_HASH_TABLE_QUERY_BATCH];

    int tableId = tupleIds[0].tableId;
    int HashTableCount = CatalogHashTableCountGet(tableId);
    for (int i = 0; i < HashTableCount; i++) {
        BuildGetOrInsertReqOnHashTableIndex(batchSize, tableId, tupleIds,
                                            resultTupleIds, resultCounterpart);
    }
}

void SendGetOrInsertReq(struct dpu_set_t set, uint32_t tableId,
                        HashTableId hashTableId, Key *keys,
                        uint64_t *tupleAddrs, size_t batchSize,
                        uint8_t *recvBuffers[]) {
    // prepare dpu buffers

    // end block
    for (int i = 0; i < NUM_DPU; i++) {
        BufferBuilderEndBlock(&builders[i]);
        buffers[i] = BufferBuilderFinish(&builders[i], &sizes[i]);
    }
    // get max sizes
    qsort(sizes, NUM_DPU, sizeof(size_t), cmpfunc);
    // send
    struct dpu_set_t dpu;

    uint32_t idx;
    DPU_FOREACH(set, dpu, idx) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, buffers[idx]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "receiveBuffer", 0, sizes[0],
                             DPU_XFER_DEFAULT));
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    // ReadDpuSetLog(set);
    // receive
    DPU_FOREACH(set, dpu, idx) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, recvBuffers[idx]));
    }
    // how to get the reply buffer size? it seems that the reply buffer size
    // will less then send buffer size
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "replyBuffer", 0, sizes[0],
                             DPU_XFER_DEFAULT));
    // free
    for (int i = 0; i < NUM_DPU; i++) {
        free(buffers[i]);
    }
}