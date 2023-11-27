#include "../protocol.h"
#include "cpu_buffer_builder.h"
#include <string.h>
#include <stdlib.h>

void CreateCpuToDpuBufferForEachDPU()
{
  uint8_t *buffer;
  size_t size;
  BufferBuilder builder, *B;
  B = &builder;
  Task *tasks[BATCH_SIZE];
  GetOrInsertReq *reqs[BATCH_SIZE];
  for (int i = 0; i < BATCH_SIZE; i++) {
    char key = (char)('a' + i);
    reqs[i] = (GetOrInsertReq*)malloc(ROUND_UP_TO_8(1) + sizeof(GetOrInsertReq));
    memcpy(reqs[i], &(GetOrInsertReq) {
      .base = { .taskType = GET_OR_INSERT_REQ },
      .len = 1,
      .tid = { .tableId = i, .tupleAddr = i },
      .hashTableId = i % 64
      }, sizeof(GetOrInsertReq));
    memcpy(reqs[i]->ptr, &key, 1);
    tasks[i] = (Task*)reqs[i];
  }
  CpuToDpuBufferDescriptor bufferDesc = {
    .header = {
      .epochNumber = 1,
    }
  };
  BufferBuilderInit(B, &bufferDesc);
  // suppose we have only one block
  BufferBuilderBeginBlock(B, GET_OR_INSERT_REQ);
  // input stream to get task
  for (int i = 0; i < BATCH_SIZE; i++) {
    BufferBuilderAppendTask(B, tasks[i]);
  }
  BufferBuilderEndBlock(B);
  buffer = BufferBuilderFinish(B, &size);
  for (int i = 0; i < BATCH_SIZE; i++) {
    free(reqs[i]);
  }
  return;
}
