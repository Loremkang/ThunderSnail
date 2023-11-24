#include "requests.h"
#include "dpu.h"

#ifndef DPU_BINARY
#define DPU_BINARY "dpu_task"
#endif

int cmpfunc (const void * a, const void * b)
{
   return ( *(int*)b - *(int*)a );
}

void SendGetOrInsertReq(uint32_t tableId, Key *keys, uint64_t *tupleAddrs, size_t batchSize)
{
  ValidValueCheck(batchSize <= BATCH_SIZE * NUM_DPU);
  // prepare dpu buffers
  uint8_t *buffers[NUM_DPU];
  size_t sizes[NUM_DPU];
  BufferBuilder builders[NUM_DPU];
  CpuToDpuBufferDescriptor bufferDesc = {
    .header = {
      .epochNumber = GetEpochNumber(),
    }
  };
  for (int i = 0; i < NUM_DPU; i++) {
    BufferBuilderInit(&builders[i], &bufferDesc);
    BufferBuilderBeginBlock(&builders[i], GET_OR_INSERT_REQ);
  }
  for (int i = 0; i < batchSize; i++){
    int dpuIdx = i % NUM_DPU;
    GetOrInsertReq req = {
      .base = { .taskType = GET_OR_INSERT_REQ },
      .len = keys[i].len,
      .tid = { .tableId = tableId, .tupleAddr = tupleAddrs[i] },
      .hashTableId = i % NUM_DPU
    };
    memcpy(&req + sizeof(GetOrInsertReq), &keys[i].data, keys[i].len);
  // append one task for each dpu
    BufferBuilderAppendTask(&builders[i], (Task*)&req);
  }
  // end block
  for (int i = 0; i < NUM_DPU; i++) {
    BufferBuilderEndBlock(&builders[i]);
    buffers[i] = BufferBuilderFinish(&builders[i], &sizes[i]);
  }
  // get max sizes
  qsort(sizes, NUM_DPU, sizeof(size_t), cmpfunc);
  
  // send
  struct dpu_set_t set, dpu;

  DPU_ASSERT(dpu_alloc(NUM_DPU, NULL, &set));
  DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));

  DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
  size_t idx = 0;
  DPU_FOREACH(set, dpu) {
    DPU_ASSERT(dpu_prepare_xfer(dpu, &buffers[idx]));
    idx++;
  }
  DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "receiveBuffer", 0, sizes[0], DPU_XFER_DEFAULT));
  DPU_ASSERT(dpu_free(set));
}
