#include "requests_handler.h"
#include "iterators.h"
#include "task_def.h"

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

void PrintInsertResp(GetOrInsertResp *resp) {
  switch(resp->tupleIdOrMaxLinkAddr.type) {
  case TupleId: {
    // printf("rid=%d\t", resp->taskidx);
    printf("(TupleIdT){.tableId = %d, \t.tupleAddr = 0x%lx}\n", resp->tupleIdOrMaxLinkAddr.value.tupleId.tableId, resp->tupleIdOrMaxLinkAddr.value.tupleId.tupleAddr);
    break;
  }
  case MaxLinkAddr: {
    // printf("rid=%d\t", resp->taskidx);
    printf("(MaxLinkAddrT) {.dpuId = %d,\t.dpuAddr = 0x%x}\n", resp->tupleIdOrMaxLinkAddr.value.maxLinkAddr.rPtr.dpuId, resp->tupleIdOrMaxLinkAddr.value.maxLinkAddr.rPtr.dpuAddr);
    break;
  }
  case HashAddr: {
    // printf("rid=%d\t", resp->taskidx);
    printf("(HashAddrT) {.dpuId = %d, \t.dpuAddr = 0x%x}\n", resp->tupleIdOrMaxLinkAddr.value.hashAddr.rPtr.dpuId, resp->tupleIdOrMaxLinkAddr.value.hashAddr.rPtr.dpuAddr);
    break;
  }
  default:
    printf("PrintInsertResp Error\n");
    ValidValueCheck(false);
  }
}
void ProcessTask(uint8_t *taskPtr, uint8_t taskType)
{
  switch(taskType) {
  case GET_OR_INSERT_RESP: {
    GetOrInsertResp *resp = (GetOrInsertResp*)taskPtr;
    PrintInsertResp(resp);
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
    OffsetsIterator taskIterator = OffsetsIteratorInit(VariableLengthTask, blockPtr, offsetsPtr, taskCount, 0);
    while (OffsetsIteratorHasNext(&taskIterator)) {
      uint8_t *taskPtr = OffsetsIteratorGetData(&taskIterator);
      ProcessTask(taskPtr, taskType);
      OffsetsIteratorNext(&taskIterator);
    }
    OffsetsIteratorReset(&taskIterator);
  } else {
    uint32_t eachTaskSize = (GetBlockTotalSize(blockPtr) - DPU_BUFFER_HEAD_LEN) / taskCount;
    for(int i = 0; i < taskCount; i++){
      uint8_t *taskPtr = blockPtr + sizeof(BlockDescriptorBase) + eachTaskSize * i;
      ProcessTask(taskPtr, taskType);
    }
  }
}

void TraverseReceiveBuffer(uint8_t *buffer)
{
  Offset *offsetsPtr = GetBufferOffsetsPtr(buffer);
  OffsetsIterator blockIterator = OffsetsIteratorInit(VariableLengthTask, buffer, offsetsPtr, GetBlockCnt(buffer), 0);
  while (OffsetsIteratorHasNext(&blockIterator)) {
    uint8_t *blockPtr = OffsetsIteratorGetData(&blockIterator);
    TraverseBlock(blockPtr);
    OffsetsIteratorNext(&blockIterator);
  }
  OffsetsIteratorReset(&blockIterator);
}

OffsetsIterator BlockIteratorInit(uint8_t *buffer)
{
  Offset *offsetsPtr = GetBufferOffsetsPtr(buffer);
  return OffsetsIteratorInit(VariableLengthTask, buffer, offsetsPtr, GetBlockCnt(buffer), 0);
}

OffsetsIterator TaskIteratorInit(OffsetsIterator *blockIterator) {
  OffsetsIterator taskIterator;
  if (!OffsetsIteratorHasNext(blockIterator)) {
    return OffsetsIteratorInit(FixedLengthTask, NULL, NULL, 0, 1);
  }
  uint8_t *blockPtr = OffsetsIteratorGetData(blockIterator);
  uint8_t taskType = GetTaskType(blockPtr);
  uint16_t taskCount = GetTaskCount(blockPtr);
  if (taskCount == 0) {
    return OffsetsIteratorInit(FixedLengthTask, NULL, NULL, 0, 1);
  }
  if(IsVarLenTask(taskType)){
    Offset *offsetsPtr = GetBlockOffsetsPtr(blockPtr);
    return OffsetsIteratorInit(VariableLengthTask, blockPtr, offsetsPtr, taskCount, 0);
  } else {
    uint32_t eachTaskSize = (GetBlockTotalSize(blockPtr) - DPU_BUFFER_HEAD_LEN) / taskCount;
    return OffsetsIteratorInit(FixedLengthTask, blockPtr + sizeof(BlockDescriptorBase), (Offset*)NULL, taskCount, eachTaskSize);
  }
}