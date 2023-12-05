#include "cpu_buffer_builder.h"
#include <string.h>
#include <stdlib.h>

void BufferBuilderInit(BufferBuilder *builder, CpuToDpuBufferDescriptor *bufferDesc)
{
  // alloc the whole buffer
  bufferDesc->header.blockCnt = 0;
  bufferDesc->header.totalSize = CPU_BUFFER_HEAD_LEN;
  builder->bufferDesc = bufferDesc;
  builder->buffer = (uint8_t*)malloc(BUFFER_LEN);
  memset(builder->buffer, 0, BUFFER_LEN);

  builder->curBlockOffset = CPU_BUFFER_HEAD_LEN;
  builder->curBlockPtr = builder->buffer + builder->curBlockOffset;

  builder->curTaskPtr = NULL;
  builder->curTaskOffset = 0;
  builder->varLenBlockIdx = 0;
  builder->fixedLenBlockIdx = 0;
  builder->isCurVarLenBlock = false;

  // offsets
  bufferDesc->offsets = malloc(sizeof(Offset) * NUM_BLOCKS);
  memset(bufferDesc->offsets, 0, sizeof(Offset) * NUM_BLOCKS);
  return;
}

void BufferBuilderBeginBlock(BufferBuilder *builder, uint8_t taskType)
{
  builder->bufferDesc->offsets[builder->bufferDesc->header.blockCnt++] = builder->curBlockOffset;
  builder->bufferDesc->header.totalSize += sizeof(Offset);
  // fill block header
  // fill task type, skip two fields
  *builder->curBlockPtr = taskType;
  builder->curTaskPtr = builder->curBlockPtr + BLOCK_HEAD_LEN;
  builder->curTaskOffset = BLOCK_HEAD_LEN;

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
    builder->bufferDesc->fixedLenBlockDescs[builder->fixedLenBlockIdx].blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
  }
  // update total size of buffer
  builder->bufferDesc->header.totalSize += sizeof(BlockDescriptorBase);
}

void BufferBuilderEndBlock(BufferBuilder *builder)
{
  // flush offsets of block
  if (builder->isCurVarLenBlock) {
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx++];
    uint32_t offsetsLen = varLenBlockDesc->blockDescBase.taskCount * sizeof(Offset);
    uint8_t* offsetsBegin = builder->curBlockPtr + varLenBlockDesc->blockDescBase.totalSize - offsetsLen;
    memcpy(offsetsBegin, varLenBlockDesc->offsets, offsetsLen);
    uint32_t paddingOffsetsLen = ROUND_UP_TO_8(offsetsLen);
    // update total size
    varLenBlockDesc->blockDescBase.totalSize += paddingOffsetsLen - offsetsLen;
    builder->bufferDesc->header.totalSize += paddingOffsetsLen - offsetsLen;
    builder->curBlockOffset += varLenBlockDesc->blockDescBase.totalSize;
    free(varLenBlockDesc->offsets);
    // flush block header
    memcpy(builder->curBlockPtr, &varLenBlockDesc->blockDescBase, sizeof(BlockDescriptorBase));
  } else {
    FixedLenBlockDescriptor* fixedLenBlockDesc = &builder->bufferDesc->fixedLenBlockDescs[builder->fixedLenBlockIdx++];
    builder->curBlockOffset += fixedLenBlockDesc->blockDescBase.totalSize;
    // flush block header
    memcpy(builder->curBlockPtr, &fixedLenBlockDesc->blockDescBase, sizeof(BlockDescriptorBase));
  }
  builder->curBlockPtr = builder->buffer + builder->curBlockOffset;
  // flush buffer header
  memcpy(builder->buffer, &builder->bufferDesc->header, sizeof(CpuBufferHeader));
}

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size)
{
  // flush offsets of buffer
  *size = builder->bufferDesc->header.totalSize;
  uint32_t offsetsLen = builder->bufferDesc->header.blockCnt * sizeof(Offset);
  uint8_t* offsetsBegin = builder->buffer + *size - offsetsLen;
  memcpy(offsetsBegin, builder->bufferDesc->offsets, offsetsLen);
  *size = ROUND_UP_TO_8(*size);
  builder->bufferDesc->header.totalSize = *size;
  // flush buffer header
  memcpy(builder->buffer, &builder->bufferDesc->header, sizeof(CpuBufferHeader));
  // free
  free(builder->bufferDesc->offsets);
  return builder->buffer;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{
  switch(task->taskType) {
  case GET_OR_INSERT_REQ:
  case GET_POINTER_REQ:
  case MERGE_MAX_LINK_REQ:
    {
    // record the offset and task count++
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc->varLenBlockDescs[builder->varLenBlockIdx];
    varLenBlockDesc->offsets[varLenBlockDesc->blockDescBase.taskCount++] = builder->curTaskOffset;
    uint32_t taskSize = GetFixedLenTaskSize(task);
    memcpy(builder->curTaskPtr, task, taskSize);
    builder->curTaskPtr += taskSize;
    builder->curTaskOffset += taskSize;
    // updata total size
    varLenBlockDesc->blockDescBase.totalSize += taskSize + sizeof(Offset);
    builder->bufferDesc->header.totalSize += taskSize + sizeof(Offset);
    break;
  }
  case SET_DPU_ID_REQ:
  case CREATE_INDEX_REQ:
  case UPDATE_POINTER_REQ:
  case GET_MAX_LINK_SIZE_REQ:
  case FETCH_MAX_LINK_REQ:
    {
    // record the offset and task count++
    FixedLenBlockDescriptor* fixedLenBlockDesc = &builder->bufferDesc->fixedLenBlockDescs[builder->fixedLenBlockIdx];
    fixedLenBlockDesc->blockDescBase.taskCount++;
    uint32_t taskSize = GetFixedLenTaskSize(task);
    memcpy(builder->curTaskPtr, task, taskSize);
    builder->curTaskPtr += taskSize;
    builder->curTaskOffset += taskSize;
    // updata total size
    fixedLenBlockDesc->blockDescBase.totalSize += taskSize;
    builder->bufferDesc->header.totalSize += taskSize;
    break;
  }

  default:
    Unimplemented("other tasks to be impl!\n");
    break;
  }
}
