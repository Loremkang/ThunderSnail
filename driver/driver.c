#include <inttypes.h>
#include "driver.h"
#include "fake_interface.h"
#include "safety_check_macro.h"
#include "cpu_buffer_builder.h"
#include "protocol.h"
#include "hash_function.h"
#include "get_or_insert_result_to_newlink.h"

#define PRIMARY_INDEX_MAX_NUM 16

// called in system startup
void DriverInit(DriverT *driver) {
    DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &driver->dpu_set));
    DPU_ASSERT(dpu_load(driver->dpu_set, DPU_BINARY, NULL));
    SendSetDpuIdReq(driver->dpu_set);

    InitCatalog(&driver->catalog);
    HashTableForNewLinkInit(&driver->ht);
    VariableLengthStructBufferInit(&driver->newLinkBuffer);
    VariableLengthStructBufferInit(&driver->preMaxLinkBuffer);

    IOManagerInit(&driver->ioManager, &driver->dpu_set, GlobalIOBuffers, GlobalOffsetsBuffer,
                GlobalVarlenBlockOffsetBuffer, GlobalIOBuffers);
    memset(driver->validMaxLinkCount, 0, sizeof(driver->validMaxLinkCount));
    driver->newMaxLinkCount = 0;
}

// size_t RunGetOrInsert(CatalogT* catalog, IOManagerT *ioManager, struct dpu_set_t* set, int batchSize, int tableId, TupleIdT *tupleIds,
//                          TupleIdT *resultTupleIds,
//                          HashTableQueryReplyT *resultCounterpart) {
//     int hashTableCount = CatalogHashTableCountGet(catalog, tableId);
//     size_t resultCount = 0;
//     IOManagerStartBufferBuild(ioManager);
//     IOManagerBeginBlock(ioManager, GET_OR_INSERT_REQ);
//     for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
//         ValidValueCheck(batchSize > 0);
//         int hashTableId = CatalogEdgeIdGet(tableId, hashTableIdx);
//         int keyLength = CatalogHashTableKeyLengthGet(hashTableId);
//         uint8_t *key = malloc(keyLength);
//         size_t taskSize = ROUND_UP_TO_8(keyLength + sizeof(GetOrInsertReq));
//         GetOrInsertReq *req = malloc(taskSize);
//         memset(req, 0, taskSize);
//         req->base = (Task){.taskType = GET_OR_INSERT_REQ};
//         req->len = keyLength;
//         req->hashTableId = hashTableId;
//         for (int i = 0; i < batchSize; i++) {
//             CatalogHashTableKeyGet(tupleIds[i], hashTableIdx, key);
//             int dpuIdx = _hash_function(key, keyLength) % NUM_DPU;
//             // printf("Task %d (key: %" PRIx64", len: %d) goes to dpu %d\n", resultCount, *(uint64_t*)key, keyLength, dpuIdx);
//             req->tid = tupleIds[i];
//             memcpy(req->ptr, key, keyLength);
//             req->taskIdx = resultCount;
//             // append one task for each dpu
//             IOManagerAppendTask(ioManager, dpuIdx, (Task *)req);
//             resultTupleIds[resultCount] = tupleIds[i];
//             resultCount ++;
//         }
//         free(key);
//         free(req);
//     }
//     IOManagerEndBlock(ioManager);
//     IOManagerFinish(ioManager);
//     IOManagerSendExecReceive(ioManager);
// #ifdef DEBUG
//     bool used[MAXSIZE_HASH_TABLE_QUERY_BATCH] = {false};
// #endif
//     for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
//         OffsetsIterator blockIterator = BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
//         ValidValueCheck(blockIterator.size == 1);
//         for (; OffsetsIteratorHasNext(&blockIterator);
//              OffsetsIteratorNext(&blockIterator)) {
//             OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
//             for (; OffsetsIteratorHasNext(&taskIterator);
//                  OffsetsIteratorNext(&taskIterator)) {
//                 uint8_t *task = OffsetsIteratorGetData(&taskIterator);
//                 GetOrInsertResp *resp = (GetOrInsertResp *)task;
//                 resultCounterpart[resp->taskIdx] = resp->tupleIdOrMaxLinkAddr;
//                 ValidValueCheck(resp->base.taskType == GET_OR_INSERT_RESP);
//                 ValidValueCheck(used[resp->taskIdx] == false);
//                 #ifdef DEBUG
//                 used[resp->taskIdx] = true;
//                 #endif
//             }
//         }
//     }
// #ifdef DEBUG
//     for (int i = 0; i < resultCount; i++) {
//         ValidValueCheck(used[i]);
//     }
// #endif
//     return resultCount;
// }

size_t RunGetOrInsertWithKeys(CatalogT* catalog, IOManagerT *ioManager, struct dpu_set_t* set, int batchSize, int tableId, TupleIdT *tupleIds,
                         TupleIdT *resultTupleIds,
                         HashTableQueryReplyT *resultCounterpart, KeyT* keys) {
    printf("===== %s =====\n", __func__);
    int hashTableCount = CatalogHashTableCountGet(catalog, tableId);

    size_t resultCount = 0;

    IOManagerStartBufferBuild(ioManager);
    IOManagerBeginBlock(ioManager, GET_OR_INSERT_REQ);

    int keyCount = 0;
    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        ValidValueCheck(batchSize > 0);
        int hashTableId = CatalogEdgeIdGet(catalog, tableId, hashTableIdx);
        // uint8_t *key = malloc(keyLength);

        int keyLength = keys[keyCount].size;
        size_t taskSize = ROUND_UP_TO_8(keyLength + sizeof(GetOrInsertReq));
        GetOrInsertReq *req = malloc(taskSize);
        memset(req, 0, taskSize);
        req->base = (Task){.taskType = GET_OR_INSERT_REQ};
        req->len = keyLength;
        req->hashTableId = hashTableId;

        for (int i = 0; i < batchSize; i++) {
            memcpy(req->ptr, keys[keyCount].buf, keyLength);
            int dpuIdx = _hash_function(req->ptr, keyLength) % NUM_DPU;
            // printf("Task %d (key: %" PRIx64", len: %d) goes to dpu %d\n", resultCount, *(uint64_t*)key, keyLength, dpuIdx);
            req->tid = tupleIds[i];
            req->taskIdx = resultCount;
            IOManagerAppendTask(ioManager, dpuIdx, (Task *)req);
            resultTupleIds[resultCount] = tupleIds[i];
            resultCount ++;
            keyCount ++;
        }
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
        ValidValueCheck(blockIterator.size == 1);
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                GetOrInsertResp *resp = (GetOrInsertResp *)task;
                resultCounterpart[resp->taskIdx] = resp->tupleIdOrMaxLinkAddr;

                ValidValueCheck(resp->base.taskType == GET_OR_INSERT_RESP);
                ValidValueCheck(used[resp->taskIdx] == false);
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

void PrintHashTableQueryReply(HashTableQueryReplyT value) {
    switch (value.type) {
        case TupleId: {
            TupleIdPrint(value.value.tupleId);
            break;
        }
        case MaxLinkAddr: {
            MaxLinkAddrPrint(value.value.maxLinkAddr);
            break;
        }
        case HashAddr: {
            HashAddrPrint(value.value.hashAddr);
            break;
        }
        default:
            printf("PrintHashTableQueryReply Error\n");
            ValidValueCheck(false);
    }
}

void PrintGetOrInsertResult(CatalogT* catalog, int batchSize, int resultSize, int tableId,
                            TupleIdT *resultTupleIds,
                            HashTableQueryReplyT *resultCounterpart) {
    printf("===== %s =====\n", __func__);
    int hashTableCount = CatalogHashTableCountGet(catalog, tableId);
    int resultCount = 0;
    for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
        ValidValueCheck(batchSize > 0);
        int hashTableId = CatalogEdgeIdGet(catalog, tableId, hashTableIdx);
        printf("\n\n========== Table %d ========== Edge %d ==========\n", tableId, hashTableId);
        for (int i = 0; i < batchSize; i++) {
            printf("----- i = %d -----\n", i);
            // uint64_t val;
            // ValidValueCheck(sizeof(val) == CatalogHashTableKeyLengthGet(hashTableId));
            // CatalogHashTableKeyGet(resultTupleIds[resultCount], hashTableIdx, (uint8_t*)&val);
            printf("Edge ID = %d\n", hashTableId);
            // printf("Value = %" PRIx64 "\n", val);
            TupleIdPrint(resultTupleIds[resultCount]);
            PrintHashTableQueryReply(resultCounterpart[resultCount]);
            resultCount ++;
        }
    }
    ValidValueCheck(resultCount == resultSize);
}

// void PrintGetOrInsertResultWithKeys(CatalogT* catalog, int batchSize, int resultSize, int tableId,
//                             TupleIdT *resultTupleIds,
//                             HashTableQueryReplyT *resultCounterpart, KeyT* keys) {
//     int hashTableCount = CatalogHashTableCountGet(catalog, tableId);
//     int resultCount = 0;
//     int keyCount = 0;
//     for (int hashTableIdx = 0; hashTableIdx < hashTableCount; hashTableIdx++) {
//         ValidValueCheck(batchSize > 0);
//         int hashTableId = CatalogEdgeIdGet(catalog, tableId, hashTableIdx);
//         printf("\n\n========== Table %d ========== Edge %d ==========\n", tableId, hashTableId);
//         for (int i = 0; i < batchSize; i++) {
//             printf("----- i = %d -----\n", i);
//             uint64_t val;
//             ValidValueCheck(sizeof(val) == CatalogHashTableKeyLengthGet(hashTableId));
//             // CatalogHashTableKeyGet(resultTupleIds[resultCount], hashTableIdx, (uint8_t*)&val);
//             ValidValueCheck(sizeof(uint64_t) == keys[keyCount].size);
//             memcpy(&val, keys[keyCount].buf, sizeof(uint64_t));
//             printf("Edge ID = %d\n", hashTableId);
//             printf("Value = %" PRIx64 "\n", val);
//             TupleIdPrint(resultTupleIds[resultCount]);
//             PrintHashTableQueryReply(resultCounterpart[resultCount]);
//             resultCount ++;
//         }
//     }
//     ValidValueCheck(resultCount == resultSize);
// }

void GetMaximumMaxLinkInNewLink(IOManagerT *ioManager,
                                VariableLengthStructBufferT *newLinkBuffer,
                                int *maxLinkSize, int *largestMaxLinkPos,
                                int *largestMaxLinkSize) {
    printf("===== %s =====\n", __func__);
    ArrayOverflowCheck(newLinkBuffer->count <= MAXSIZE_HASH_TABLE_QUERY_BATCH);
    for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
        largestMaxLinkPos[i] = -1;
        largestMaxLinkSize[i] = 0;
    }

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
            // MaxLinkAddrPrint(addr);
            IOManagerAppendTask(ioManager, dpuIdx, (Task *)(&task));
        }
    }
    printf("Get Maxlink Size Task Count = %d\n", taskCount);

    // run tasks : 1 IO Round
    IOManagerEndBlock(ioManager);
    IOManagerFinish(ioManager);

    IOManagerSendExecReceive(ioManager);
    // ReadDpuSetLog(*ioManager->dpu_set);

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
                ValidValueCheck(resp->base.taskType == GET_MAX_LINK_SIZE_RESP);
            }
        }
    }

    int taskIdx = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
        NewLinkT *newLink =
            (NewLinkT *)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j++) {
            int size = maxLinkSize[taskIdx++];
            ValidValueCheck(size > 0);
            if (largestMaxLinkPos[i] == -1 || size > largestMaxLinkSize[i]) {
                largestMaxLinkPos[i] = j;
                largestMaxLinkSize[i] = size;
            }
        }
    }
    for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
        NewLinkT *newLink =
            (NewLinkT *)VariableLengthStructBufferGet(newLinkBuffer, i);
        if (newLink->maxLinkAddrCount > 0) {
            ValidValueCheck(largestMaxLinkPos[i] >= 0 && largestMaxLinkPos[i] < newLink->maxLinkAddrCount);
            // printf("i = %d;\t size = %d\n", i, largestMaxLinkSize[i]);
        }
    }
    ValidValueCheck(taskIdx == taskCount);
}

void PrintMaximumMaxLinkInfo(VariableLengthStructBufferT *newLinkBuffer,
                             int *maxLinkSize, int *largestMaxLinkPos,
                             int *largestMaxLinkSize) {
    printf("===== %s =====\n", __func__);
    int resultCount = 0;
    for (int i = 0; i < newLinkBuffer->count; i ++) {
        NewLinkT* newLink = (NewLinkT*) VariableLengthStructBufferGet(newLinkBuffer, i);
        NewLinkPrint(newLink);
        for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
            printf("Size = %d; Addr = ", maxLinkSize[resultCount ++]);
            MaxLinkAddrPrint(NewLinkGetMaxLinkAddrs(newLink)[j]);
        }
        printf("\n");
        printf("Max Size at [%d] = %d\n", largestMaxLinkPos[i], largestMaxLinkSize[i]);
        printf("\n");
    }
    // ValidValueCheck(resultCount == )
}

void GetPreMaxLinkFromNewLink(IOManagerT *ioManager,
                              VariableLengthStructBufferT *newLinkBuffer,
                              VariableLengthStructBufferT *preMaxLinkBuffer,
                              NewLinkMergerT *merger, int *idx,
                              int *largestMaxLinkPos, int *largestMaxLinkSize) {
    printf("===== %s =====\n", __func__);
    ArrayOverflowCheck(newLinkBuffer->count <= MAXSIZE_HASH_TABLE_QUERY_BATCH);
    memset(idx, -1, sizeof(*idx) * newLinkBuffer->count);
    VariableLengthStructBufferSoftReset(preMaxLinkBuffer);

    IOManagerStartBufferBuild(ioManager);
    IOManagerBeginBlock(ioManager, FETCH_MAX_LINK_REQ);

    int taskCount = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
        NewLinkT *newLink =
            (NewLinkT *)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j++) {
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
    // ReadDpuSetLog(*ioManager->dpu_set);

    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator =
            BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        ValidValueCheck(blockIterator.size == 1);
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
            ValidValueCheck(blockIterators[dpuId].size == 1);
            taskIterators[dpuId] = TaskIteratorInit(&blockIterators[dpuId]);
        }

        for (OffsetT i = 0; i < newLinkBuffer->count; i++) {
            NewLinkT *newLink =
                (NewLinkT *)VariableLengthStructBufferGet(newLinkBuffer, i);
            NewLinkMergerReset(merger);
            NewLinkMergeNewLink(merger, newLink);
            merger->maxLinkAddrCount = 0;
            for (int j = 0; j < newLink->maxLinkAddrCount; j++) {
                if (j == largestMaxLinkPos[i]) {
                    merger->maxLinkAddrs[merger->maxLinkAddrCount++] =
                        NewLinkGetMaxLinkAddrs(newLink)[j];
                } else {
                    int taskIdxInBlock = idx[taskIdx++];
                    MaxLinkAddrT addr = NewLinkGetMaxLinkAddrs(newLink)[j];
                    int dpuId = addr.rPtr.dpuId;
                    uint8_t *task = OffsetsIteratorGetKthData(
                        &taskIterators[dpuId], taskIdxInBlock);
                    FetchMaxLinkResp *resp = (FetchMaxLinkResp *)task;
                    // MaxLinkAddrPrint(addr);
                    // printf("(MaxLink) TupleCount = %d;\t HashAddrCount = %d\n", resp->maxLink.tupleIDCount, resp->maxLink.hashAddrCount);
                    NewLinkMergeMaxLink(merger, &resp->maxLink);
                }
            }
            size_t size = NewLinkMergerGetExportSize(merger);
            Offset offset = VariableLengthStructBufferAppendPlaceholder(
                preMaxLinkBuffer, size);
            NewLinkT *preMaxLink = (NewLinkT *)VariableLengthStructBufferGet(
                preMaxLinkBuffer, offset);
            NewLinkMergerExport(merger, preMaxLink);
        }
        ValidValueCheck(taskIdx == taskCount);
    }
}

void PrintPreMaxLinkInfo(VariableLengthStructBufferT* buf) {
    printf("===== %s =====\n", __func__);
    printf(" ===== PreMaxLink Count = %d ===== \n", buf->count);
    for (int i = 0; i < buf->count; i ++) {
        NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(buf, i);
        NewLinkPrint(newLink);
        printf("\n");
    }
}

void InsertPreMaxLink(IOManagerT *ioManager,
                      VariableLengthStructBufferT *preMaxLinkBuffer,
                      MaxLinkAddrT *newMaxLinkAddrs, int *nextNewMaxLinkCount) {
    ArrayOverflowCheck(preMaxLinkBuffer->count <=
                       MAXSIZE_HASH_TABLE_QUERY_BATCH);
    printf("===== %s =====\n", __func__);

    IOManagerStartBufferBuild(ioManager);
    // New MaxLink
    int newMaxLinkTaskCount = 0;
    {
        uint8_t taskType = NEW_MAX_LINK_REQ;
        IOManagerBeginBlock(ioManager, taskType);
        for (OffsetT i = 0; i < preMaxLinkBuffer->count; i++) {
            NewLinkT *preMaxLink =
                (NewLinkT *)VariableLengthStructBufferGet(preMaxLinkBuffer, i);
            if (preMaxLink->maxLinkAddrCount == 0) {
                // new MaxLink
                int dpuId = hash32((*nextNewMaxLinkCount)++) % NUM_DPU;
                size_t size = ROUND_UP_TO_8(sizeof(NewMaxLinkReq) +
                              preMaxLink->hashAddrCount * sizeof(HashAddrT) +
                              preMaxLink->tupleIDCount * sizeof(TupleIdT));
                uint8_t *task = IOManagerAppendPlaceHolder(ioManager, dpuId,
                                                           taskType, size);
                NewMaxLinkReq *req = (NewMaxLinkReq *)task;
                req->base.taskType = taskType;
                req->taskIdx = i;
                NewLinkToMaxLink(preMaxLink, &req->maxLink);
                // assert(size % 4 == 0);
                // for (int i = 0; i < size / 4; i ++) {
                //     int *x = (int*) req;
                //     printf("%x ", x[i]);
                // }
                // printf("size = %zu\n", size);
                newMaxLinkTaskCount ++;
                #ifdef DEBUG
                newMaxLinkAddrs[i].rPtr = INVALID_REMOTEPTR;
                #endif
            }
        }
        IOManagerEndBlock(ioManager);
    }

    printf("New MaxLink Task Count : %d\n", newMaxLinkTaskCount);

    // #ifdef DEBUG
    // for (int i = 0; i < taskCount; i ++) {
    //     newMaxLinkAddrs[i].rPtr = INVALID_REMOTEPTR;
    // }
    // #endif

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
                uint8_t *task = IOManagerAppendPlaceHolder(ioManager, dpuId,
                                                           taskType, size);
                MergeMaxLinkReq *req = (MergeMaxLinkReq *)task;
                req->base.taskType = taskType;
                req->ptr = addr.rPtr;
                NewLinkToMaxLink(preMaxLink, &req->maxLink);
                // printf("Target MaxLink Addr: ");
                // MaxLinkAddrPrint(addr);
                // printf("Size = %d\n", size);
                // printf("HashCount = %d; \tTupleIdCount = %d\n", req->maxLink.hashAddrCount, req->maxLink.tupleIDCount);
                // for (int j = 0; j < req->maxLink.hashAddrCount; j ++) {
                //     HashAddrPrint(MaxLinkGetHashAddrs(&req->maxLink)[j]);
                // }
                // for (int j = 0; j < req->maxLink.tupleIDCount; j ++) {
                //     TupleIdPrint(MaxLinkGetTupleIDs(&req->maxLink)[j]);
                // }
            }
        }
        IOManagerEndBlock(ioManager);
    }

    IOManagerFinish(ioManager);

    // run tasks : 1 IO Round
    IOManagerSendExecReceive(ioManager);
    // ReadDpuSetLog(*ioManager->dpu_set);

    // Merge MaxLink has no reply
    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator =
            BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        ValidValueCheck(blockIterator.size == 2);
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
                // printf("taskidx = %d\n", resp->taskIdx);
            }
        }
    }

    // #ifdef DEBUG
    // for (int i = 0; i < preMaxLinkBuffer->count; i ++) {
    //     MaxLinkAddrT invalidMaxLinkAddr = (MaxLinkAddrT){.rPtr = INVALID_REMOTEPTR};
    //     ValidValueCheck(!MaxLinkAddrEqual(invalidMaxLinkAddr, newMaxLinkAddrs[i]));
    // }
    // #endif
}

void PrintNewMaxLinkAddrs(VariableLengthStructBufferT *preMaxLinkBuffer, MaxLinkAddrT *newMaxLinkAddrs) {
    for (int i = 0; i < preMaxLinkBuffer->count; i ++) {
        NewLinkT *preMaxLink =
                (NewLinkT *)VariableLengthStructBufferGet(preMaxLinkBuffer, i);
        if (preMaxLink->maxLinkAddrCount == 0) {
            ValidValueCheck(!RemotePtrInvalid(newMaxLinkAddrs[i].rPtr));
            printf("New MaxLink [%d] = ", i);
            MaxLinkAddrPrint(newMaxLinkAddrs[i]);
        }
    }
}

uint32_t UpdateHashTableAndGetResult(IOManagerT *ioManager,
                     VariableLengthStructBufferT *preMaxLinkBuffer,
                     MaxLinkAddrT *newMaxLinkAddrs, int *validMaxLinkCount) {
    ArrayOverflowCheck(preMaxLinkBuffer->count <=
                       MAXSIZE_HASH_TABLE_QUERY_BATCH);
    printf("===== %s =====\n", __func__);

    IOManagerStartBufferBuild(ioManager);

    {
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
            for (int hashIdx = 0; hashIdx < preMaxLink->hashAddrCount; hashIdx++) {
                HashAddrT hashAddr = NewLinkGetHashAddrs(preMaxLink)[hashIdx];
                UpdatePointerReq task;
                memset(&task, 0, sizeof(task));
                task.base = (Task){.taskType = taskType};
                task.hashAddr = hashAddr;
                int dpuId = hashAddr.rPtr.dpuId;
                task.maxLinkAddr = addr;
                // HashAddrPrint(hashAddr);
                // MaxLinkAddrPrint(addr);
                IOManagerAppendTask(ioManager, dpuId, (Task *)(&task));
            }
        }
        IOManagerEndBlock(ioManager);
    }
    {
        uint8_t taskType = GET_VALID_MAXLINK_COUNT_REQ;
        IOManagerBeginBlock(ioManager, taskType);
        for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
            GetValidMaxLinkCountReq req;
            req.base.taskType = taskType;
            IOManagerAppendTask(ioManager, dpuId, (Task*)(&req));
        }
        IOManagerEndBlock(ioManager);
    }

    IOManagerFinish(ioManager);

    IOManagerSendExecReceive(ioManager);
    // ReadDpuSetLog(*ioManager->dpu_set);

    uint32_t totalValidMaxLinkCount = 0;
    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator =
            BlockIteratorInit(ioManager->recvIOBuffers[dpuId]);
        ValidValueCheck(blockIterator.size == 2);
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            ValidValueCheck(taskIterator.size <= 1);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                GetValidMaxLinkCountResp *resp = (GetValidMaxLinkCountResp *)task;
                validMaxLinkCount[dpuId] = resp->count;
                totalValidMaxLinkCount += resp->count;
                ValidValueCheck(resp->base.taskType == GET_VALID_MAXLINK_COUNT_RESP);
            }
        }
    }
    return totalValidMaxLinkCount;
}

void PrintValidMaxLinkCount(int *validMaxLinkCount) {
    printf("===== %s =====\n", __func__);
    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        if (validMaxLinkCount[dpuId] > 0) {
            printf("Count of DPU [%d] = %d\n", dpuId, validMaxLinkCount[dpuId]);
        }
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

// uint32_t DriverBatchInsertTuple(DriverT *driver, int batchSize,
//                             TupleIdT *tupleIds) {
//     BatchInsertValidCheck(batchSize, tupleIds);
//     int tableId = tupleIds[0].tableId;
//     int HashTableCount = CatalogHashTableCountGet(&driver->catalog, tableId);
//     size_t resultCount = RunGetOrInsert(&driver->catalog,
//         &driver->ioManager, &driver->dpu_set, batchSize, tableId, tupleIds,
//         driver->resultTupleIds, driver->resultCounterpart);
//     PrintGetOrInsertResult(batchSize, resultCount, tableId,
//         driver->resultTupleIds, driver->resultCounterpart);
//     GetOrInsertResultToNewlink(resultCount, driver->resultTupleIds,
//                                driver->resultCounterpart, &driver->ht,
//                                &driver->newLinkBuffer);
//     PrintNewLinkResult(&driver->newLinkBuffer);
//     GetMaximumMaxLinkInNewLink(&driver->ioManager, &driver->newLinkBuffer,
//                                driver->maxLinkSize, driver->largestMaxLinkPos,
//                                driver->largestMaxLinkSize);
//     PrintMaximumMaxLinkInfo(&driver->newLinkBuffer, driver->maxLinkSize,
//                             driver->largestMaxLinkPos,
//                             driver->largestMaxLinkSize);
//     GetPreMaxLinkFromNewLink(&driver->ioManager, &driver->newLinkBuffer,
//                              &driver->preMaxLinkBuffer, &driver->merger,
//                              driver->idx, driver->largestMaxLinkPos,
//                              driver->largestMaxLinkSize);
//     PrintPreMaxLinkInfo(&driver->preMaxLinkBuffer);
//     InsertPreMaxLink(&driver->ioManager, &driver->preMaxLinkBuffer,
//                      driver->newMaxLinkAddrs);
//     PrintNewMaxLinkAddrs(&driver->preMaxLinkBuffer, driver->newMaxLinkAddrs);
//     driver->totalValidMaxLinkCount = UpdateHashTableAndGetResult(&driver->ioManager, &driver->preMaxLinkBuffer,
//                     driver->newMaxLinkAddrs, driver->validMaxLinkCount);
//     PrintValidMaxLinkCount(driver->validMaxLinkCount);
//     return driver->totalValidMaxLinkCount;
// }

uint32_t DriverBatchInsertTupleWithKeys(DriverT *driver, int batchSize,
                            TupleIdT *tupleIds, KeyT* keys) {
    BatchInsertValidCheck(batchSize, tupleIds);

    int tableId = tupleIds[0].tableId;
    int HashTableCount = CatalogHashTableCountGet(&driver->catalog, tableId);

    size_t resultCount = RunGetOrInsertWithKeys(&driver->catalog,
        &driver->ioManager, &driver->dpu_set, batchSize, tableId, tupleIds,
        driver->resultTupleIds, driver->resultCounterpart, keys);
    // PrintGetOrInsertResult(&driver->catalog, batchSize, resultCount, tableId,
    //     driver->resultTupleIds, driver->resultCounterpart);

    GetOrInsertResultToNewlink(resultCount, driver->resultTupleIds,
                               driver->resultCounterpart, &driver->ht,
                               &driver->newLinkBuffer);

    // PrintNewLinkResult(&driver->newLinkBuffer);

    GetMaximumMaxLinkInNewLink(&driver->ioManager, &driver->newLinkBuffer,
                               driver->maxLinkSize, driver->largestMaxLinkPos,
                               driver->largestMaxLinkSize);

    // PrintMaximumMaxLinkInfo(&driver->newLinkBuffer, driver->maxLinkSize,
    //                         driver->largestMaxLinkPos,
    //                         driver->largestMaxLinkSize);

    GetPreMaxLinkFromNewLink(&driver->ioManager, &driver->newLinkBuffer,
                             &driver->preMaxLinkBuffer, &driver->merger,
                             driver->idx, driver->largestMaxLinkPos,
                             driver->largestMaxLinkSize);
    
    // PrintPreMaxLinkInfo(&driver->preMaxLinkBuffer);

    InsertPreMaxLink(&driver->ioManager, &driver->preMaxLinkBuffer,
                     driver->newMaxLinkAddrs, &driver->newMaxLinkCount);
    
    // PrintNewMaxLinkAddrs(&driver->preMaxLinkBuffer, driver->newMaxLinkAddrs);

    driver->totalValidMaxLinkCount = UpdateHashTableAndGetResult(&driver->ioManager, &driver->preMaxLinkBuffer,
                    driver->newMaxLinkAddrs, driver->validMaxLinkCount);
    
    PrintValidMaxLinkCount(driver->validMaxLinkCount);
    
    return driver->totalValidMaxLinkCount;
}

void DriverFree(DriverT *driver) {
    CatalogFree(&driver->catalog);
    HashTableForNewLinkFree(&driver->ht);
    VariableLengthStructBufferFree(&driver->newLinkBuffer);
    VariableLengthStructBufferFree(&driver->preMaxLinkBuffer);
    DPU_ASSERT(dpu_free(driver->dpu_set));
}