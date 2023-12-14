#include <gtest/gtest.h>

extern "C" {
// #include "../protocol/cpu/requests.h"
// #include "../protocol/cpu/requests_handler.h"
#include "cpu_buffer_builder.h"
#include "hash_function.h"
#include "requests_handler.h"
#include "requests.h"
#include "io_manager.h"
// #include "iterators.h"
}

#define TEST_BATCH (8 * 1024)

TEST(IOManager, IOManager) {
    struct dpu_set_t set;
    HashTableId hashTableId = 0;

    // init
    DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
    SendSetDpuIdReq(set);
    SendCreateIndexReq(set, hashTableId);

    TupleIdT tupleIdT1[TEST_BATCH];
    TupleIdT tupleIdT2[TEST_BATCH];
    uint32_t keys[TEST_BATCH];
    for (uint32_t i = 0; i < TEST_BATCH; i++) {
        tupleIdT1[i] = (TupleIdT){.tableId = 1, .tupleAddr = i};
        tupleIdT2[i] = (TupleIdT){.tableId = 2, .tupleAddr = i};
        keys[i] = i;
    }

    IOManagerT ioManager;
    IOManagerInit(&ioManager, &set, GlobalIOBuffers, GlobalOffsetsBuffer,
                  GlobalVarlenBlockOffsetBuffer, GlobalIOBuffers);
    IOManagerStartBufferBuild(&ioManager);
    IOManagerBeginBlock(&ioManager, GET_OR_INSERT_REQ);

    for (int i = 0; i < TEST_BATCH; i++) {
        uint32_t key = keys[i];
        uint32_t keysize = sizeof(uint32_t);
        int dpuIdx = key % NUM_DPU;
        size_t taskSize = ROUND_UP_TO_8(keysize + sizeof(GetOrInsertReq));
        GetOrInsertReq *req = (GetOrInsertReq *)malloc(taskSize);
        memset(req, 0, taskSize);
        req->base = (Task){.taskType = GET_OR_INSERT_REQ};
        req->len = keysize;
        req->tid = {.tableId = tupleIdT1[i].tableId,
                    .tupleAddr = tupleIdT1[i].tupleAddr};
        req->hashTableId = hashTableId;
        req->taskIdx = i;
        memcpy(req->ptr, &key, sizeof(uint32_t));
        IOManagerAppendTask(&ioManager, dpuIdx, (Task *)req);
    }

    for (int i = 0; i < TEST_BATCH; i++) {
        uint32_t key = keys[i];
        uint32_t keysize = sizeof(uint32_t);
        // int dpuIdx = hash32(key) % NUM_DPU;
        int dpuIdx = key % NUM_DPU;
        size_t taskSize = ROUND_UP_TO_8(keysize + sizeof(GetOrInsertReq));
        GetOrInsertReq *req = (GetOrInsertReq *)malloc(taskSize);
        memset(req, 0, taskSize);
        req->base = (Task){.taskType = GET_OR_INSERT_REQ};
        req->len = keysize;
        req->tid = {.tableId = tupleIdT2[i].tableId,
                    .tupleAddr = tupleIdT2[i].tupleAddr};
        req->hashTableId = hashTableId;
        req->taskIdx = i + TEST_BATCH;
        memcpy(req->ptr, &key, sizeof(uint32_t));
        IOManagerAppendTask(&ioManager, dpuIdx, (Task *)req);
    }

    IOManagerEndBlock(&ioManager);
    IOManagerFinish(&ioManager);
    IOManagerSendExecReceive(&ioManager);

    GetOrInsertResp responses[TEST_BATCH * 2];
    bool used[TEST_BATCH * 2];
    memset(responses, 0, sizeof(responses));
    memset(used, 0, sizeof(used));

    for (int dpuId = 0; dpuId < NUM_DPU; dpuId++) {
        OffsetsIterator blockIterator = BlockIteratorInit(ioManager.recvIOBuffers[dpuId]);
        for (; OffsetsIteratorHasNext(&blockIterator);
             OffsetsIteratorNext(&blockIterator)) {
            OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
            for (; OffsetsIteratorHasNext(&taskIterator);
                 OffsetsIteratorNext(&taskIterator)) {
                uint8_t *task = OffsetsIteratorGetData(&taskIterator);
                GetOrInsertResp *resp = (GetOrInsertResp *)task;
                EXPECT_FALSE(used[resp->taskIdx]);
                used[resp->taskIdx] = true;
                memcpy(responses + resp->taskIdx, resp,
                       sizeof(GetOrInsertResp));
            }
        }
    }

    for (int i = 0; i < TEST_BATCH; i++) {
        int other = i + TEST_BATCH;
        EXPECT_TRUE(used[i]);
        EXPECT_TRUE(used[other]);
        EXPECT_NE(responses[i].tupleIdOrMaxLinkAddr.type, MaxLinkAddr);
        switch (responses[i].tupleIdOrMaxLinkAddr.type) {
            case TupleId:
                EXPECT_EQ(responses[other].tupleIdOrMaxLinkAddr.type, HashAddr);
                EXPECT_EQ(
                    responses[i].tupleIdOrMaxLinkAddr.value.tupleId.tableId, 2);
                EXPECT_EQ(
                    responses[i].tupleIdOrMaxLinkAddr.value.tupleId.tupleAddr,
                    i);
                break;
            case HashAddr:
                EXPECT_EQ(responses[other].tupleIdOrMaxLinkAddr.type, TupleId);
                EXPECT_EQ(
                    responses[other].tupleIdOrMaxLinkAddr.value.tupleId.tableId,
                    1);
                EXPECT_EQ(responses[other]
                              .tupleIdOrMaxLinkAddr.value.tupleId.tupleAddr,
                          i);
                break;
            default:
                FAIL();
                break;
        }
    }

    // for (int i = 0; i < TEST_BATCH * 2; i ++) {
    //   if (used[i]) {
    //     ProcessTask((uint8_t*)&responses[i], GET_OR_INSERT_RESP);
    //   }
    // }

    DPU_ASSERT(dpu_free(set));
}