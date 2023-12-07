#include <gtest/gtest.h>

extern "C" {
// #include "../protocol/cpu/requests.h"
// #include "../protocol/cpu/requests_handler.h"
#include "cpu_buffer_builder.h"
#include "hash_function.h"
#include "requests_handler.h"
#include "requests.h"
// #include "iterators.h"
}

#define TEST_BATCH (8 * 1024)

TEST (BufferHandler, SetDpuIdReq) {
  struct dpu_set_t set;
  DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
  DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
  SendSetDpuIdReq(set);
  DPU_ASSERT(dpu_free(set));
}

TEST (BufferHandler, CreateIndex) {
  struct dpu_set_t set;
  HashTableId hashTableId = 0;
  DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
  DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
  SendCreateIndexReq(set, hashTableId);
  DPU_ASSERT(dpu_free(set));
}

TEST (BufferHandler, DecodeBuffer) {
  struct dpu_set_t set;
  HashTableId hashTableId = 0;
  DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
  DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
  SendSetDpuIdReq(set);
  SendCreateIndexReq(set, hashTableId);
  uint64_t tupleAddrs[TEST_BATCH] =
    {0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,
     0x1234, 0x2341, 0x3412, 0x4123, 0x1234, 0x2341, 0x3412, 0x4123,};
  Key keys[TEST_BATCH];
  for (int i=0; i < TEST_BATCH; i++) {
    keys[i].data = (uint8_t *)malloc(8);
    keys[i].data[0] = 'a';
    keys[i].data[1] = 'b';
    keys[i].data[2] = 'c';
    keys[i].data[3] = 'd';
    keys[i].data[4] = '\0';
    keys[i].len = 4;
  }

  uint8_t** recvBufs;
  recvBufs = (uint8_t **)malloc(NUM_DPU * sizeof(uint8_t*));
  for (int i=0; i < NUM_DPU; i++) {
    recvBufs[i] = (uint8_t *)malloc(65535);
  }
  printf("Now calling SendGetOrInsertReq\n");
  SendGetOrInsertReq(set, 3, hashTableId, keys, tupleAddrs, TEST_BATCH, recvBufs);
  TraverseReceiveBuffer(recvBufs[0]);
  DPU_ASSERT(dpu_free(set));
}

int cmpfunc (const void * a, const void * b)
{
   return ( *(int*)b - *(int*)a );
}

TEST (BufferHandler, BlockIteratorAndTaskIterator) {
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
  for (uint32_t i = 0; i < TEST_BATCH; i ++) {
    tupleIdT1[i] = (TupleIdT){.tableId = 1, .tupleAddr = i};
    tupleIdT2[i] = (TupleIdT){.tableId = 2, .tupleAddr = i};
    keys[i] = i;
  }

  uint8_t *buffers[NUM_DPU];
  size_t sizes[NUM_DPU];
  BufferBuilder builders[NUM_DPU];
  CpuToDpuBufferDescriptor bufferDescs[NUM_DPU];
  for (int i = 0; i < NUM_DPU; i++) {
    memset(&bufferDescs[i], 0, sizeof(CpuToDpuBufferDescriptor));
    bufferDescs[i] = (CpuToDpuBufferDescriptor) {
      .header = {
	      .epochNumber = GetEpochNumber(),
      }
    };
  }
  for (int i = 0; i < NUM_DPU; i++) {
    BufferBuilderInit(&builders[i], &bufferDescs[i], i);
    BufferBuilderBeginBlock(&builders[i], GET_OR_INSERT_REQ);
  }
  for (int i = 0; i < TEST_BATCH; i ++) {
    uint32_t key = keys[i];
    uint32_t keysize = sizeof(uint32_t);
    int dpuIdx = key % NUM_DPU;
    size_t taskSize = ROUND_UP_TO_8(keysize + sizeof(GetOrInsertReq));
    GetOrInsertReq *req = (GetOrInsertReq*)malloc(taskSize);
    memset(req, 0, taskSize);
    req->base = (Task) { .taskType = GET_OR_INSERT_REQ };
    req->len = keysize;
    req->tid = {.tableId = tupleIdT1[i].tableId, .tupleAddr = tupleIdT1[i].tupleAddr};
    req->hashTableId = hashTableId;
    // req->taskidx = i;
    memcpy(req->ptr, &key, sizeof(uint32_t));
    BufferBuilderAppendTask(&builders[dpuIdx], (Task*)req);
  }

  for (int i = 0; i < TEST_BATCH; i ++) {
    uint32_t key = keys[i];
    uint32_t keysize = sizeof(uint32_t);
    // int dpuIdx = hash32(key) % NUM_DPU;
    int dpuIdx = key % NUM_DPU;
    size_t taskSize = ROUND_UP_TO_8(keysize + sizeof(GetOrInsertReq));
    GetOrInsertReq *req = (GetOrInsertReq*)malloc(taskSize);
    memset(req, 0, taskSize);
    req->base = (Task) { .taskType = GET_OR_INSERT_REQ };
    req->len = keysize;
    req->tid = {.tableId = tupleIdT2[i].tableId, .tupleAddr = tupleIdT2[i].tupleAddr};
    req->hashTableId = hashTableId;
    // req->taskidx = i;
    memcpy(req->ptr, &key, sizeof(uint32_t));
    BufferBuilderAppendTask(&builders[dpuIdx], (Task*)req);
  }

  // end block
  for (int i = 0; i < NUM_DPU; i++) {
    BufferBuilderEndBlock(&builders[i]);
    buffers[i] = BufferBuilderFinish(&builders[i], &sizes[i]);
  }
  // get max sizes
  qsort(sizes, NUM_DPU, sizeof(size_t), cmpfunc);
  printf("BlockIteratorAndTaskIterator Size :%zu\n", sizes[0]);

  // send
  struct dpu_set_t dpu;

  uint32_t idx;
  DPU_FOREACH(set, dpu, idx) {
    DPU_ASSERT(dpu_prepare_xfer(dpu, buffers[idx]));
  }
  DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "receiveBuffer", 0, sizes[0], DPU_XFER_DEFAULT));
  DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
  // ReadDpuSetLog(set);

  uint8_t** recvBufs;
  recvBufs = (uint8_t **)malloc(NUM_DPU * sizeof(uint8_t*));
  for (int i=0; i < NUM_DPU; i++) {
    recvBufs[i] = (uint8_t *)malloc(65535);
  }

  // receive
  DPU_FOREACH(set, dpu, idx) {
    DPU_ASSERT(dpu_prepare_xfer(dpu, recvBufs[idx]));
  }

  // how to get the reply buffer size? it seems that the reply buffer size will less then send buffer size
  DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "replyBuffer", 0, sizes[0], DPU_XFER_DEFAULT));

  {
    printf("%p\n", recvBufs[63]);
    uint64_t *val = (uint64_t*)recvBufs[63];
    for (int i = 0; i < 10; i ++) {
      printf("%016llx\n", val[i]);
    }
    GetOrInsertResp a;
    sizeof(a.base);
    sizeof(a.tupleIdOrMaxLinkAddr);
    sizeof(GetOrInsertResp);
    OffsetsIterator blockIterator = BlockIteratorInit(recvBufs[63]);
    for (; OffsetsIteratorHasNext(&blockIterator); OffsetsIteratorNext(&blockIterator)) {
      printf("%p\n", blockIterator.baseAddr);
      OffsetsIterator taskIterator = TaskIteratorInit(&blockIterator);
      printf("%d\t%d\n", blockIterator.size, taskIterator.index);
      printf("%d\t%d\n", taskIterator.size, taskIterator.taskSize);
      printf("%d\n", taskIterator.type);
      printf("%p\t%p\n", taskIterator.offsets, taskIterator.baseAddr);
      for (; OffsetsIteratorHasNext(&taskIterator); OffsetsIteratorNext(&taskIterator)) {
        uint8_t* task = OffsetsIteratorGetData(&taskIterator);
        ProcessTask(task, GET_OR_INSERT_RESP);
      }
    }
  }
  
  DPU_ASSERT(dpu_free(set));
}