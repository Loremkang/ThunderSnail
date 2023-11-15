#include "protocol.h"

void CreateCpuToDpuBufferForEachDPU()
{
  uint8_t *buffer;
  size_t size;
  BufferBuilder builder, *B;
  B = &builder;
  Tasks* tasks[BATCH_SIZE];
  for (int i = 0; i < BATCH_SIZE; i++) {
    GetOrInsertReq req = {
      .base = { .taskType = GET_OR_INSERT_REQ },
      .ptr = malloc(8),
      .len = 8,
      .tid = { .tableId = i, .tupleAddr = i },
      hashTableId = i % 64
    };
    tasks[i] = (Task*)&req;
  }
  CpuToDpuBufferDescriptor bufferDesc = {
    .epochNumber = 1,
  };
  BufferBuilderInit(B, &bufferDesc);
  // suppose we have only one block
  BufferBuilderBeginBlock(B, &tasks[0]);
  // input stream to get task
  for (int i = 1; i < BATCH_SIZE; i++) {
    BufferBuilderAppendTask(B, &task[i]);
  }
  BufferBuilderEndBlock(B);
  buffer = BufferBuilderFinish(B, &size);
  return;
}
