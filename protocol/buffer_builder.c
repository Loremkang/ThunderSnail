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
    builder->isCurVarLenBlock = true;
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx];
    varLenBlockDesc->blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
    // how many tasks?
    varLenBlockDesc->offsets = malloc(sizeof(Offset) * BATCH_SIZE);
  } else {
    builder->isCurVarLenBlock = false;
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
  // flush offsets of block
  if (builder->isCurVarLenBlock) {
    uint32_t offsetsLen = builder->totalTasks * sizeof(Offset);
    VarLenBlockDescriptor* varLenBlockDesc = builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx++];
    uint8_t* offsetsBegin = varLenBlockDesc.blockDescBase.totalSize - offsetsLen;
    memcpy(offsetsBegin, varLenBlockDesc->offsets, offsetsLen);
  }
}

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size)
{
  // flush offsets of buffer
  *size = builder->bufferDesc->totalSize;
  uint32_t offsetsLen = builder->totalBlocks * sizeof(Offset);
  uint8_t* offsetsBegin = *size - offsetsLen;
  memcpy(offsetsBegin, builder->bufferDesc->offsets, offsetsLen);
  return builder->buffer;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{
  builder->totalTasks++;
  switch(task->taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    // record the offset and task count++
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx];
    varLenBlockDesc->offsets[varLenBlockDesc->blockDescBase.taskCount++] = builder->curOffset;
    // key len
    *builder->curPtr = req->len;
    builder->curPtr++;
    builder->curOffset++;

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
    // TODO impl other tasks
  default:
    break;
  }
}
