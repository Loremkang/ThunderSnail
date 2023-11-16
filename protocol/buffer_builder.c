#include "buffer_builder.h"
#include <string.h>


void BufferBuilderInit(BufferBuilder *builder, CpuToDpuBufferDescriptor *bufferDesc)
{
  // alloc the whole buffer
  bufferDesc->blockCnt = 0;
  bufferDesc->totalSize = BUFFER_HEAD_LEN;
  builder->bufferDesc = bufferDesc;
  builder->buffer = malloc(BUFFER_LEN);
  *builder->buffer = bufferDesc->epochNumber;
  // skip blockCnt and totalSize, later to fill them
  builder->curPtr = builder->buffer + BUFFER_HEAD_LEN;
  builder->curOffset = BUFFER_HEAD_LEN;

  builder->varLenBlockIdx = 0;
  builder->fixedLenBlockIdx = 0;
  builder->totalBlocks = 0;

  // offsets
  bufferDesc->offsets = malloc(sizeof(Offset) * NUM_BLOCKS);
  return;
}

void BufferBuilderBeginBlock(BufferBuilder *builder, Task* firstTask)
{
  builder->bufferDesc->offsets[builder->totalBlocks++] = builder->curOffset;
  builder->bufferDesc->totalSize += sizeof(Offset);
  // fill block header
  // fill task type, skip two fields
  uint8_t taskType = firstTask->taskType;
  *builder->curPtr = taskType;
  builder->curPtr += BLOCK_HEAD_LEN;
  builder->curOffset += BLOCK_HEAD_LEN;


  // block descriptor fill
  if (IsVarLenTask(taskType)) {
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx++];
    varLenBlockDesc->blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
    // how many tasks?
    varLenBlockDesc->offsets = malloc(sizeof(Offset) * BATCH_SIZE);
  } else {
    builder->bufferDesc->fixedLenBlockDescs[builder->fixedLenBlockIdx++].blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
  }
  // update total size of buffer
  builder->bufferDesc->totalSize += sizeof(BlockDescriptorBase);
  // fill buffer,  first task
  BufferBuilderAppendTask(builder, firstTask);
}

void BufferBuilderEndBlock(BufferBuilder *builder)
{
  // update blockCnt and flush to buffer
  *(builder->buffer + sizeof(uint8_t)) = ++builder->bufferDesc->blockCnt;
  // update total size of the block and flush
  *(builder->buffer + sizeof(uint8_t) * 2) = builder->bufferDesc->totalSize;
}

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size)
{
  *size = builder->bufferDesc->totalSize;
  return builder->buffer;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{
  switch(task->taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    // record the offset and task count++
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx];
    varLenBlockDesc->offsets[varLenBlockDesc->blockDescBase.taskCount++] = builder->curOffset;
    // key
    memcpy(builder->curPtr, req->ptr, req->len);
    builder->curPtr += req->len;
    builder->curOffset += req->len;
    
    // value, tid
    *((TupleIdT*)builder->curPtr) = req->tid;
    builder->curPtr += sizeof(TupleIdT);
    builder->curOffset += sizeof(TupleIdT);

    // updata total size
    uint16_t taskSize = GetVarLenTaskSize(task);
    varLenBlockDesc->blockDescBase.totalSize += taskSize;
    builder->bufferDesc->totalSize += taskSize;
    break;
  }
  default:
    break;
  }
}
