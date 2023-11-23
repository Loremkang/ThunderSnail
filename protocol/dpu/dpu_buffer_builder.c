#include "dpu_buffer_builder.h"
#include <alloc.h>
#include <stdlib.h>
#include <string.h>

void BufferBuilderInit(BufferBuilder *builder)
{
  buddy_init(BUDDY_LEN);
  // alloc the whole buffer
  builder->bufferDesc.blockCnt = 0;
  builder->bufferDesc.totalSize = DPU_BUFFER_HEAD_LEN;
  // need clear the reply buffer?
  memset(replyBuffer, 0, sizeof(replyBuffer));
  replyBuffer[0] = BUFFER_STATE_OK;

  // skip blockCnt and totalSize, later to fill them
  builder->curBlockPtr = (uint8_t*)replyBuffer + DPU_BUFFER_HEAD_LEN;
  builder->curBlockOffset = DPU_BUFFER_HEAD_LEN;

  builder->varLenBlockIdx = 0;
  builder->fixedLenBlockIdx = 0;

  // offsets reset
  memset(builder->bufferDesc.offsets, 0, sizeof(builder->bufferDesc.offsets));
  return;
}

void BufferBuilderBeginBlock(BufferBuilder *builder, uint8_t taskType)
{
  builder->bufferDesc.offsets[builder->bufferDesc.blockCnt++] = builder->curBlockOffset;
  builder->bufferDesc.totalSize += sizeof(Offset);
  // fill block header
  // fill task type, skip two fields
  *builder->curBlockPtr = taskType;
  builder->curTaskPtr = builder->curBlockPtr + BLOCK_HEAD_LEN;
  builder->curTaskOffset = BLOCK_HEAD_LEN;

  // block descriptor fill
  if (IsVarLenTask(taskType)) {
    builder->isCurVarLenBlock = true;
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc.varLenBlockDescs[builder->varLenBlockIdx];
    varLenBlockDesc->blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
    // each time alloc NUM_OF_TASKS
    varLenBlockDesc->offsets = buddy_alloc(sizeof(Offset) * NUM_OF_TASKS);
  } else {
    builder->isCurVarLenBlock = false;
    builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx++].blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
  }
  // update total size of buffer
  builder->bufferDesc.totalSize += sizeof(BlockDescriptorBase);
}

void BufferBuilderEndBlock(BufferBuilder *builder)
{
  // update blockCnt and flush to buffer
  replyBuffer[1] = builder->bufferDesc.blockCnt;
  // update total size of the block and flush
  replyBuffer[2] = builder->bufferDesc.totalSize;
  // flush offsets of block
  if (builder->isCurVarLenBlock) {
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc.varLenBlockDescs[builder->varLenBlockIdx++];
    uint32_t offsetsLen = varLenBlockDesc->blockDescBase.taskCount * sizeof(Offset);
    uint8_t* offsetsBegin = builder->curBlockPtr + varLenBlockDesc->blockDescBase.totalSize - offsetsLen;
    memcpy(offsetsBegin, varLenBlockDesc->offsets, offsetsLen);
    builder->curBlockOffset += varLenBlockDesc->blockDescBase.totalSize;
    buddy_free(varLenBlockDesc->offsets);
  } else {
    FixedLenBlockDescriptor* fixedLenBlockDesc = &builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx++];
    builder->curBlockOffset += fixedLenBlockDesc->blockDescBase.totalSize;
  }
  builder->curBlockPtr = (uint8_t*)replyBuffer + builder->curBlockOffset;
}

size_t BufferBuilderFinish(BufferBuilder *builder)
{
  // flush offsets of buffer
  size_t size = builder->bufferDesc.totalSize;
  uint32_t offsetsLen = builder->bufferDesc.blockCnt * sizeof(Offset);
  uint8_t* offsetsBegin = (uint8_t*)replyBuffer + size - offsetsLen;
  memcpy(offsetsBegin, builder->bufferDesc.offsets, offsetsLen);
  return size;
}
