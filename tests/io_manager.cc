#include <gtest/gtest.h>

extern "C" {
// #include "protocol/cpu/requests.h"
// #include "protocol/cpu/requests_handler.h"
#include "cpu_buffer_builder.h"
#include "hash_function.h"
#include "requests_handler.h"
#include "requests.h"
#include "io_manager.h"
// #include "iterators.h"
}

#define TEST_BATCH (8 * 1024)

TEST(IOManager, SingleRound) {
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

TEST(IOManager, TwoRounds) {
    struct dpu_set_t set;
    HashTableId hashTableId = 0;

    // init
    DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
    SendSetDpuIdReq(set);
    SendCreateIndexReq(set, hashTableId);

    TupleIdT tupleIdT1[TEST_BATCH];
    TupleIdT tupleIdT2[TEST_BATCH];
    using KeyType = uint64_t;
    KeyType keys[TEST_BATCH];
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
        KeyType key = keys[i];
        size_t keysize = sizeof(KeyType);
        int dpuIdx = hash32(key) % NUM_DPU;
        size_t taskSize = ROUND_UP_TO_8(keysize + sizeof(GetOrInsertReq));
        GetOrInsertReq *req = (GetOrInsertReq *)malloc(taskSize);
        memset(req, 0, taskSize);
        req->base = (Task){.taskType = GET_OR_INSERT_REQ};
        req->len = keysize;
        req->tid = {.tableId = tupleIdT1[i].tableId,
                    .tupleAddr = tupleIdT1[i].tupleAddr};
        req->hashTableId = hashTableId;
        req->taskIdx = i;
        memcpy(req->ptr, &key, sizeof(KeyType));
        IOManagerAppendTask(&ioManager, dpuIdx, (Task *)req);
    }

    IOManagerEndBlock(&ioManager);
    IOManagerFinish(&ioManager);
    IOManagerSendExecReceive(&ioManager);


    GetOrInsertResp responses[TEST_BATCH];
    bool used[TEST_BATCH];
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
        EXPECT_TRUE(used[i]);
        EXPECT_EQ(responses[i].tupleIdOrMaxLinkAddr.type, HashAddr);
    }


    IOManagerStartBufferBuild(&ioManager);
    IOManagerBeginBlock(&ioManager, GET_OR_INSERT_REQ);

    for (int i = 0; i < TEST_BATCH; i++) {
        KeyType key = keys[i];
        size_t keysize = sizeof(KeyType);
        // int dpuIdx = hash32(key) % NUM_DPU;
        int dpuIdx = hash32(key) % NUM_DPU;
        size_t taskSize = ROUND_UP_TO_8(keysize + sizeof(GetOrInsertReq));
        GetOrInsertReq *req = (GetOrInsertReq *)malloc(taskSize);
        memset(req, 0, taskSize);
        req->base = (Task){.taskType = GET_OR_INSERT_REQ};
        req->len = keysize;
        req->tid = {.tableId = tupleIdT2[i].tableId,
                    .tupleAddr = tupleIdT2[i].tupleAddr};
        req->hashTableId = hashTableId;
        req->taskIdx = i;
        memcpy(req->ptr, &key, sizeof(KeyType));
        IOManagerAppendTask(&ioManager, dpuIdx, (Task *)req);
    }

    IOManagerEndBlock(&ioManager);
    IOManagerFinish(&ioManager);
    IOManagerSendExecReceive(&ioManager);
    
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
        EXPECT_TRUE(used[i]);
        EXPECT_EQ(responses[i].tupleIdOrMaxLinkAddr.type, TupleId);
        EXPECT_EQ(responses[i].tupleIdOrMaxLinkAddr.value.tupleId.tableId, 1);
        EXPECT_EQ(responses[i].tupleIdOrMaxLinkAddr.value.tupleId.tupleAddr, i);
        // TupleIdPrint(responses[i].tupleIdOrMaxLinkAddr.value.tupleId);
    }
    DPU_ASSERT(dpu_free(set));
}

void TwoRoundsReceive(IOManagerT *manager, int epoch, int smallReceive) {
  struct dpu_set_t dpu;
  uint32_t idx;
  for (int i=0; i < epoch; i ++) {
    DPU_FOREACH(*(manager->dpu_set), dpu, idx) {
      DPU_ASSERT(dpu_prepare_xfer(dpu, manager->recvIOBuffers[idx]));
    }

    DPU_ASSERT(dpu_push_xfer(*(manager->dpu_set), DPU_XFER_FROM_DPU, "replyBuffer", 0, sizeof(DpuBufferHeader),
                             DPU_XFER_DEFAULT));

    for (int i = 0; i < NUM_DPU; i++) {
      DpuBufferHeader* header = (DpuBufferHeader*)(manager->recvIOBuffers[i]);
      manager->recvSizes[i] = smallReceive;
    }
    manager->maxReceiveSize = ROUND_UP_TO_8(max_in_array(NUM_DPU, manager->recvSizes));

    ArrayOverflowCheck(manager->maxReceiveSize <= BUFFER_LEN);

    DPU_FOREACH(*(manager->dpu_set), dpu, idx) {
      DPU_ASSERT(dpu_prepare_xfer(dpu, manager->recvIOBuffers[idx]));
    }

    DPU_ASSERT(dpu_push_xfer(*(manager->dpu_set), DPU_XFER_FROM_DPU, "replyBuffer", 0, manager->maxReceiveSize,
                             DPU_XFER_DEFAULT));
  }
}

void OneRoundReceive(IOManagerT *manager, int epoch, int receiveSize) {
  struct dpu_set_t dpu;
  uint32_t idx;
  for (int i=0; i < epoch; i++) {
    DPU_FOREACH(*(manager->dpu_set), dpu, idx) {
      DPU_ASSERT(dpu_prepare_xfer(dpu, manager->recvIOBuffers[idx]));
    }

    DPU_ASSERT(dpu_push_xfer(*(manager->dpu_set), DPU_XFER_FROM_DPU, "replyBuffer", 0, receiveSize,
                             DPU_XFER_DEFAULT));

    for (int i = 0; i < NUM_DPU; i++) {
      DpuBufferHeader* header = (DpuBufferHeader*)manager->recvIOBuffers[i];
      manager->recvSizes[i] = receiveSize;
    }
    manager->maxReceiveSize = ROUND_UP_TO_8(max_in_array(NUM_DPU, manager->recvSizes));

    //ArrayOverflowCheck(manager->maxReceiveSize <= BUFFER_LEN);
  }
}

TEST(IOManager, ReceivePerf) {
  struct dpu_set_t set;
  // init
  DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
  DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));

  IOManagerT ioManager;
  IOManagerInit(&ioManager, &set, GlobalIOBuffers, GlobalOffsetsBuffer,
                GlobalVarlenBlockOffsetBuffer, GlobalIOBuffers);

  int receiveSize = 16384; // Size in bytes
  int smallReceive =1024;
  int epoch = 4;

  clock_t start, finish;

  while (smallReceive < BUFFER_LEN - 8) {
    // two rounds
    start = clock();
    TwoRoundsReceive(&ioManager, epoch, ROUND_UP_TO_8(BUFFER_LEN - 128));
    finish = clock();
    printf ("Buffer size: %d, two rounds time used: %.6f seconds. Per epoch: %.6f seconds\n", smallReceive,
            (double)(finish-start)/CLOCKS_PER_SEC, (double)(finish-start)/CLOCKS_PER_SEC/epoch);
    smallReceive += 2048;
  }

  while (receiveSize < BUFFER_LEN - 8) {
    // one round
    start = clock();
    OneRoundReceive(&ioManager, epoch, ROUND_UP_TO_8(BUFFER_LEN - 128));
    finish = clock();
    printf ("Buffer size: %d, one round time used: %.6f seconds. Per epoch: %.6f seconds\n", receiveSize,
            (double)(finish-start)/CLOCKS_PER_SEC, (double)(finish-start)/CLOCKS_PER_SEC/epoch);
    receiveSize += 2048;
  }
}