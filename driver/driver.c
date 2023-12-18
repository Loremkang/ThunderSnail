#include "driver.h"
#include "fake_interface.h"
#include "safety_check_macro.h"
#include "cpu_buffer_builder.h"
#include "protocol.h"
#include "hash_function.h"

#define PRIMARY_INDEX_MAX_NUM 16

// called in system startup
void DriverInit(DriverT *driver) {

    DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &driver->dpu_set));
    DPU_ASSERT(dpu_load(driver->dpu_set, DPU_BINARY, NULL));
    SendSetDpuIdReq(driver->dpu_set);

    HashTableForNewLinkInit(&driver->ht);
    VariableLengthStructBufferInit(&driver->newLinkBuffer);
    VariableLengthStructBufferInit(&driver->preMaxLinkBuffer);

    IOManagerInit(&driver->ioManager, &driver->dpu_set, GlobalIOBuffers, GlobalOffsetsBuffer,
                GlobalVarlenBlockOffsetBuffer, GlobalIOBuffers);
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

    IOManagerStartBufferBuild(ioManager);
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

void GetMaximumMaxLinkInNewLink(IOManagerT *ioManager,
                                VariableLengthStructBufferT *newLinkBuffer,
                                int *maxLinkSize,
                                int *largestMaxLinkPos,
                                int *largestMaxLinkSize) {
    ArrayOverflowCheck(newLinkBuffer->count <= MAXSIZE_HASH_TABLE_QUERY_BATCH);

    IOManagerStartBufferBuild(ioManager);
    IOManagerBeginBlock(ioManager, GET_MAX_LINK_SIZE_REQ);

    int taskCount = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
        NewLinkT *newLink =
            (NewLinkT *)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j++) {
            MaxLinkAddrT addr = NewLinkGetMaxLinkAddrs(newLink)[j];
            int dpuIdx = addr.rPtr.dpuId;
            GetMaxLinkSizeReq task;
            ValidValueCheck(GetMaxLinkSizeReq_fixed == true);
            task.base = (Task){.taskType = GET_MAX_LINK_SIZE_REQ};
            task.taskIdx = taskCount++;
            task.maxLinkAddr = addr;
            IOManagerAppendTask(ioManager, dpuIdx, (Task *)(&task));
        }
    }

    // run tasks : 1 IO Round
    IOManagerEndBlock(ioManager);
    IOManagerFinish(ioManager);
    IOManagerSendExecReceive(ioManager);

    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator =
            BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                GetMaxLinkSizeResp *resp = (GetMaxLinkSizeResp *)task;
                maxLinkSize[resp->taskIdx] = resp->maxLinkSize;
                ValidValueCheck(resp->base.taskType == GET_MAX_LINK_SIZE_REQ);
            }
        }
    }

    int taskIdx = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
        largestMaxLinkPos[i] = -1;
        largestMaxLinkSize[i] = 0;
        NewLinkT *newLink =
            (NewLinkT *)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j++) {
            int size = maxLinkSize[taskIdx ++];
            ValidValueCheck(size > 0);
            if (largestMaxLinkPos[i] == -1 || size > largestMaxLinkSize[i]) {
                largestMaxLinkPos[i] = j;
                largestMaxLinkSize[i] = size;
            }
        }
    }
    ValidValueCheck(taskIdx == taskCount);
}

void GetPreMaxLinkFromNewLink(IOManagerT *ioManager,
                                VariableLengthStructBufferT *newLinkBuffer,
                                VariableLengthStructBufferT *preMaxLinkBuffer,
                                NewLinkMergerT *merger,
                                int *idx,
                                int *largestMaxLinkPos,
                                int *largestMaxLinkSize) {
    ArrayOverflowCheck(newLinkBuffer->count <= MAXSIZE_HASH_TABLE_QUERY_BATCH);
    memset(idx, -1, sizeof(idx));

    IOManagerStartBufferBuild(ioManager);
    IOManagerBeginBlock(ioManager, FETCH_MAX_LINK_REQ);

    int taskCount = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i ++) {
        NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
            if (j != largestMaxLinkPos[i]) {
                MaxLinkAddrT addr = NewLinkGetMaxLinkAddrs(newLink)[j];
                int dpuIdx = addr.rPtr.dpuId;
                FetchMaxLinkReq task;
                ValidValueCheck(FetchMaxLinkReq_fixed == true);
                task.base = (Task){.taskType = FETCH_MAX_LINK_REQ};
                task.taskIdx = taskCount++;
                task.maxLinkAddr = addr;
                IOManagerAppendTask(ioManager, dpuIdx, (Task *)(&task));
            }
        }
    }

    // run tasks : 1 IO Round
    IOManagerEndBlock(ioManager);
    IOManagerFinish(ioManager);
    IOManagerSendExecReceive(ioManager);

    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator =
            BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        ValidValueCheck(blockIterator.count == 1);
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                FetchMaxLinkResp *resp = (FetchMaxLinkResp *)task;
                idx[resp->taskIdx] = taskIterator.index;
                ValidValueCheck(resp->base.taskType == FETCH_MAX_LINK_RESP);
            }
        }
    }

    {
        int taskIdx = 0;
        OffsetsIterator blockIterators[NUM_DPU], taskIterators[NUM_DPU];
        for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
            blockIterators[dpuId] =
                BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
            ValidValueCheck(blockIterators[dpuId].count == 1);
            taskIterators[dpuId] = TaskIteratorInit(&blockIterators[dpuId]);
        }

        for (OffsetT i = 0; i < newLinkBuffer->count; i ++) {
            NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkBuffer, i);
            NewLinkMergerReset(merger);
            for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
                if (j == largestMaxLinkPos[i]) {
                    merger->maxLinkAddrs[merger->maxLinkAddrCount ++] = NewLinkGetMaxLinkAddrs(newLink)[j];
                } else {
                    int taskIdxInBlock = idx[taskIdx ++];
                    MaxLinkAddrT addr = NewLinkGetMaxLinkAddrs(newLink)[j];
                    int dpuId = addr.rPtr.dpuId;
                    uint8_t *task = OffsetsIteratorGetKthData(
                        &taskIterators[dpuId], taskIdxInBlock);
                    FetchMaxLinkResp *resp = (FetchMaxLinkResp *)task;
                    NewLinkMerge(merger, &resp->maxLink);
                }
            }
            size_t size = NewLinkMergerGetExportSize(merger);
            Offset offset = VariableLengthStructBufferAppendPlaceholder(preMaxLinkBuffer, size);
            NewLinkT* preMaxLink =
                (NewLinkT*)VariableLengthStructBufferGet(preMaxLinkBuffer, idx);
            NewLinkMergerExport(merger, preMaxLink);
        }
        ValidValueCheck(taskIdx == taskCount);
    }
}

void InsertPreMaxLink(
    IOManagerT *ioManager, VariableLengthStructBufferT *preMaxLinkBuffer, MaxLinkAddrT* newMaxLinkAddrs,
    VariableLengthStructBufferT *validResultBuffer) {
    ArrayOverflowCheck(preMaxLinkBuffer->count <=
                       MAXSIZE_HASH_TABLE_QUERY_BATCH);

    IOManagerStartBufferBuild(ioManager);
    
    // New MaxLink
    int taskCount = 0;
    {
        uint8_t taskType = NEW_MAX_LINK_REQ;
        IOManagerBeginBlock(ioManager, taskType);
        for (OffsetT i = 0; i < preMaxLinkBuffer->count; i++) {
            NewLinkT *preMaxLink =
                (NewLinkT *)VariableLengthStructBufferGet(preMaxLinkBuffer, i);
            if (preMaxLink->maxLinkAddrCount == 0) {
                // new MaxLink
                int dpuId = i % NUM_DPU; // TODO: NOTE !!! should be rand here
                size_t size = sizeof(NewMaxLinkReq) +
                            preMaxLink->hashAddrCount * sizeof(HashAddrT) +
                            preMaxLink->tupleIDCount * sizeof(TupleIdT);
                uint8_t* task = IOManagerAppendPlaceHolder(ioManager, dpuId, taskType, size);
                NewMaxLinkReq* req = (NewMaxLinkReq*)task;
                req->taskIdx = taskCount ++;
                NewLinkToMaxLink(preMaxLink, &req->maxLink);
            }
        }
        IOManagerEndBlock(ioManager);
    }

    // Merge MaxLink
    {
        uint8_t taskType = MERGE_MAX_LINK_REQ;
        IOManagerBeginBlock(ioManager, taskType);
        for (OffsetT i = 0; i < preMaxLinkBuffer->count; i++) {
            NewLinkT *preMaxLink =
                (NewLinkT *)VariableLengthStructBufferGet(preMaxLinkBuffer, i);
            if (preMaxLink->maxLinkAddrCount != 0) {
                // new MaxLink
                ValidValueCheck(preMaxLink->maxLinkAddrCount == 1);
                MaxLinkAddrT addr = NewLinkGetMaxLinkAddrs(preMaxLink)[0];
                int dpuId = addr.rPtr.dpuId;
                size_t size = sizeof(MergeMaxLinkReq) +
                            preMaxLink->hashAddrCount * sizeof(HashAddrT) +
                            preMaxLink->tupleIDCount * sizeof(TupleIdT);
                uint8_t* task = IOManagerAppendPlaceHolder(ioManager, dpuId, taskType, size);
                MergeMaxLinkReq* req = (MergeMaxLinkReq*)task;
                req->ptr = addr.rPtr;
                NewLinkToMaxLink(preMaxLink, &req->maxLink);
            }
        }
        IOManagerEndBlock(ioManager);
    }

    IOManagerFinish(ioManager);

    // run tasks : 1 IO Round
    IOManagerSendExecReceive(ioManager);

    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator =
            BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        ValidValueCheck(blockIterator.count == 1); 
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                NewMaxLinkResp *resp = (NewMaxLinkResp *)task;
                int taskIdx = resp->taskIdx;
                NewLinkT *preMaxLink =
                    (NewLinkT *)VariableLengthStructBufferGet(preMaxLinkBuffer,
                                                              taskIdx);
                ValidValueCheck(preMaxLink->maxLinkAddrCount == 0);
                ValidValueCheck(resp->base.taskType == NEW_MAX_LINK_RESP);
                newMaxLinkAddrs[taskIdx].rPtr = resp->ptr;
            }
        }
    }
}

void UpdateHashTable(IOManagerT *ioManager,
                     VariableLengthStructBufferT *preMaxLinkBuffer,
                     MaxLinkAddrT *newMaxLinkAddrs) {
    ArrayOverflowCheck(preMaxLinkBuffer->count <=
                       MAXSIZE_HASH_TABLE_QUERY_BATCH);

    IOManagerStartBufferBuild(ioManager);
    
        uint8_t taskType = UPDATE_POINTER_REQ;
        IOManagerBeginBlock(ioManager, taskType);
        for (OffsetT i = 0; i < preMaxLinkBuffer->count; i++) {
            NewLinkT *preMaxLink =
                (NewLinkT *)VariableLengthStructBufferGet(preMaxLinkBuffer, i);
            MaxLinkAddrT addr;
            if (preMaxLink->maxLinkAddrCount == 0) {
                // New MaxLink
                addr = newMaxLinkAddrs[i];
            } else {
                // Merged MaxLink
                addr = NewLinkGetMaxLinkAddrs(preMaxLink)[0];
            }
            int dpuId = addr.rPtr.dpuId;
            for (int hashIdx = 0; hashIdx < preMaxLink->hashAddrCount; hashIdx ++) {
                HashAddrT hashAddr = NewLinkGetHashAddrs(preMaxLink)[hashIdx];
                UpdatePointerReq task;
                task.base = (Task){.taskType = taskType};
                task.hashEntry = hashAddr;
                task.maxLinkAddr = addr;
                IOManagerAppendTask(ioManager, dpuId, (Task *)(&task));
            }
        }
        IOManagerEndBlock(ioManager);

    IOManagerFinish(ioManager);

    IOManagerSendExecReceive(ioManager);
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

void DriverBatchInsertTuple(DriverT *driver, int batchSize,
                            TupleIdT *tupleIds) {
    BatchInsertValidCheck(batchSize, tupleIds);

    int tableId = tupleIds[0].tableId;
    int HashTableCount = CatalogHashTableCountGet(tableId);

    size_t resultCount = RunGetOrInsert(
        &driver->ioManager, &driver->dpu_set, batchSize, tableId, tupleIds,
        driver->resultTupleIds, driver->resultCounterpart);

    GetOrInsertResultToNewlink(resultCount, driver->resultTupleIds,
                               driver->resultCounterpart, &driver->ht,
                               &driver->newLinkBuffer);
    
    return;

    GetMaximumMaxLinkInNewLink(&driver->ioManager, &driver->newLinkBuffer,
                               driver->maxLinkSize, driver->largestMaxLinkPos,
                               driver->largestMaxLinkSize);

    GetPreMaxLinkFromNewLink(&driver->ioManager, &driver->newLinkBuffer,
                             &driver->preMaxLinkBuffer, &driver->merger,
                             driver->idx, driver->largestMaxLinkPos,
                             driver->largestMaxLinkSize);

    InsertPreMaxLink(&driver->ioManager, &driver->preMaxLinkBuffer,
                     driver->newMaxLinkAddrs, &driver->validResultBuffer);

    UpdateHashTable(&driver->ioManager, &driver->preMaxLinkBuffer, driver->newMaxLinkAddrs);
}

void DriverFree(DriverT *driver) {
    HashTableForNewLinkFree(&driver->ht);
    VariableLengthStructBufferFree(&driver->newLinkBuffer);
    VariableLengthStructBufferFree(&driver->preMaxLinkBuffer);
    DPU_ASSERT(dpu_free(driver->dpu_set));
}