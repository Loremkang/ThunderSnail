#include "requests_handler.h"

void GetBufferHeader(uint8_t *buffer, DpuBufferHeader *header)
{
  header = (DpuBufferHeader*)buffer;
}

uint8_t GetBufferState(uint8_t *buffer)
{
  return ((DpuBufferHeader*)buffer)->bufferState;
}

uint8_t GetBlockCnt(uint8_t *buffer)
{
  return ((DpuBufferHeader*)buffer)->blockCnt;
}

uint32_t GetBufferTotalSize(uint8_t *buffer)
{
  return ((DpuBufferHeader*)buffer)->totalSize;
}

void GetBlockDescriptorBase(uint8_t *blockPtr, BlockDescriptorBase *header)
{
  header = (BlockDescriptorBase*)blockPtr;
}

uint8_t GetTaskType(uint8_t *blockPtr)
{
  return ((BlockDescriptorBase*)blockPtr)->taskType;
}

uint16_t GetTaskCount(uint8_t *blockPtr)
{
   return ((BlockDescriptorBase*)blockPtr)->taskCount;
}

uint16_t GetBlockTotalSize(uint8_t *blockPtr)
{
   return ((BlockDescriptorBase*)blockPtr)->totalSize;
}

Offset* GetBufferOffsetsPtr(uint8_t *buffer)
{
  uint8_t blockCnt = GetBlockCnt(buffer);
  uint32_t totalSize = GetBufferTotalSize(buffer);
  return (Offset*)(buffer + totalSize - blockCnt * sizeof(Offset));
}
