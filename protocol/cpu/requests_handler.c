#include "requests_handler.h"
#include "iterators.h"

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
  uint16_t blockCnt = GetBlockCnt(buffer);
  uint32_t totalSize = GetBufferTotalSize(buffer);
  return (Offset*)(buffer + totalSize - ROUND_UP_TO_8(blockCnt * sizeof(Offset)));
}

Offset* GetBlockOffsetsPtr(uint8_t *blockPtr)
{
  uint16_t taskCount = GetTaskCount(blockPtr);
  uint32_t totalSize = GetBlockTotalSize(blockPtr);
  return (Offset*)(blockPtr + totalSize - ROUND_UP_TO_8(taskCount * sizeof(Offset)));
}

void* ProcessTask(uint8_t *taskPtr, uint8_t taskType)
{
  switch(taskType) {
  case GET_OR_INSERT_RESP: {
    GetOrInsertResp *resp = (GetOrInsertResp*)taskPtr;
    break;
  }
  case GET_POINTER_RESP: {
    GetPointerResp *resp= (GetPointerResp*)taskPtr;
    break;
  }
  case GET_MAX_LINK_SIZE_RESP: {
    GetMaxLinkSizeResp *resp = (GetMaxLinkSizeResp*)taskPtr;
    break;
  }
  case FETCH_MAX_LINK_RESP: {
    FetchMaxLinkResp *resp = (FetchMaxLinkResp*)taskPtr;
    break;
  }
  default:
    break;
  }
}

void TraverseBlock(uint8_t *blockPtr)
{
  uint8_t taskType = GetTaskType(blockPtr);
  uint16_t taskCount = GetTaskCount(blockPtr);
  if(IsVarLenTask(taskType)){
    Offset *offsetsPtr = GetBlockOffsetsPtr(blockPtr);
    Iterator *taskIterator = CreateOffsetsIterator(offsetsPtr, taskCount);
    while (taskIterator->hasNext(taskIterator->data)) {
      Offset *taskOffset = OffsetsIteratorGetData(taskIterator->data);
      uint8_t *taskPtr = blockPtr + *taskOffset;
      ProcessTask(taskPtr, taskType);
      taskIterator->next(taskIterator->data);
    }
    taskIterator->reset(taskIterator->data);
  } else {
    uint32_t eachTaskSize = (GetBlockTotalSize(blockPtr) - DPU_BUFFER_HEAD_LEN) / taskCount;
    for(int i = 0; i < taskCount; i++){
      uint8_t *taskPtr = blockPtr + eachTaskSize * i;
      ProcessTask(taskPtr, taskType);
    }
  }
}

void TraverseReceiveBuffer(uint8_t *buffer)
{
  Offset *offsetsPtr = GetBufferOffsetsPtr(buffer);
  Iterator *blockIterator = CreateOffsetsIterator(offsetsPtr, GetBlockCnt(buffer));
  while (blockIterator->hasNext(blockIterator->data)) {
    Offset *blockOffset = OffsetsIteratorGetData(blockIterator->data);
    uint8_t *blockPtr = buffer + *blockOffset;
    TraverseBlock(blockPtr);
    blockIterator->next(blockIterator->data);
  }
  blockIterator->reset(blockIterator->data);
}

