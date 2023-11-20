#include "protocol.h"
#include "buffer_builder.h"

void CreateCpuToDpuBufferForEachDPU()
{
  uint8_t *buffer;
  size_t size;
  BufferBuilder builder, *B;
  B = &builder;
  Task *tasks[BATCH_SIZE];
  for (int i = 0; i < BATCH_SIZE; i++) {
    GetOrInsertReq req = {
      .base = { .taskType = GET_OR_INSERT_REQ },
      .len = 8,
      .tid = { .tableId = i, .tupleAddr = i },
      .ptr = 'a' + i,
      .hashTableId = i % 64
    };
    tasks[i] = (Task*)&req;
  }
  CpuToDpuBufferDescriptor bufferDesc = {
    .epochNumber = 1,
  };
  BufferBuilderInit(B, &bufferDesc);
  // suppose we have only one block
  BufferBuilderBeginBlock(B, GET_OR_INSERT_REQ);
  // input stream to get task
  for (int i = 0; i < BATCH_SIZE; i++) {
    BufferBuilderAppendTask(B, &tasks[i]);
  }
  BufferBuilderEndBlock(B);
  buffer = BufferBuilderFinish(B, &size);
  return;
}
