#include "requests.h"

void AppendTaskToDpu(BufferBuilder *builder, Task *task)
{
}

void SendGetOrInsertReq(uint32_t tableId, Key *keys, uint64_t *tupleAddrs, size_t batchSize)
{
  ValidValueCheck(batchSize <= BATCH_SIZE * NUM_DPU);
  // prepare dpu buffers
  uint8_t *buffer[NUM_DPU];
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
    AppendTaskToDpu(&builders[i], (Task*)&req);
  }
  // end block

  // send
  
}
