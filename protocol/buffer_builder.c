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

  builder->curBlock = 0;
  return;
}

void BufferBuilderBeginBlock(BufferBuilder *builder, Task* firstTask)
{
  // fill block header
  // fill task type, skip two fields
  uint8_t taskType = firstTask->taskType;
  *builder->curPtr = taskType;
  builder->curPtr += BLOCK_HEAD_LEN;
  builder->curOffset += BLOCK_HEAD_LEN;

  // block descriptor fill
  if (IsVarLenTask(taskType)) {
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->curBlock];
    varLenBlockDesc->blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
    // how many tasks?
    varLenBlockDesc->offsets = malloc(sizeof(Offset) * BATCH_SIZE);
  } else {
    builder->bufferDesc->fixedLenBlockDescs[builder->curBlock].blockDescBase = (BlockDescriptorBase) {
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
}

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size)
{
  return builder->buffer;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{
  switch(task->taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    // record the offset and task count++
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->curBlock];
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
