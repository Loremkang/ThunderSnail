#include "dpu_buffer_builder.h"
#include <alloc.h>
#include <stdlib.h>
#include <string.h>
#include <mram.h>
#include <mram_unaligned.h>

// reply buffer
uint8_t __mram_noinit replyBuffer[BUFFER_LEN];
uint8_t __mram_noinit receiveBuffer[BUFFER_LEN];

void BufferBuilderInit(BufferBuilder *builder)
{
  buddy_init(BUDDY_LEN);
  // alloc the whole buffer
  builder->bufferDesc.header.bufferState = BUFFER_STATE_OK;
  builder->bufferDesc.header.blockCnt = 0;
  builder->bufferDesc.header.totalSize = DPU_BUFFER_HEAD_LEN;
  // need clear the reply buffer?
  //memset(replyBuffer, 0, BUFFER_LEN);

  builder->curBlockPtr = replyBuffer + DPU_BUFFER_HEAD_LEN;
  builder->curBlockOffset = DPU_BUFFER_HEAD_LEN;

  builder->varLenBlockIdx = 0;
  builder->fixedLenBlockIdx = 0;

  // offsets reset
  memset(builder->bufferDesc.offsets, 0, sizeof(builder->bufferDesc.offsets));
  return;
}

void BufferBuilderBeginBlock(BufferBuilder *builder, uint8_t taskType)
{
  builder->bufferDesc.offsets[builder->bufferDesc.header.blockCnt++] = builder->curBlockOffset;
  builder->bufferDesc.header.totalSize += sizeof(Offset);
  // fill block header
  // fill task type, skip two fields
  *builder->curBlockPtr = taskType;
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
    // each time alloc BATCH_SIZE
    varLenBlockDesc->offsets = buddy_alloc(sizeof(Offset) * BATCH_SIZE);
  } else {
    builder->isCurVarLenBlock = false;
    builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx++].blockDescBase = (BlockDescriptorBase) {
      .taskType = taskType,
      .taskCount = 0,
      .totalSize = sizeof(BlockDescriptorBase)
    };
  }
  // update total size of buffer
  builder->bufferDesc.header.totalSize += sizeof(BlockDescriptorBase);
}

void BufferBuilderEndBlock(BufferBuilder *builder)
{
  // flush offsets of block
  if (builder->isCurVarLenBlock) {
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc.varLenBlockDescs[builder->varLenBlockIdx++];
    uint32_t offsetsLen = varLenBlockDesc->blockDescBase.taskCount * sizeof(Offset);
    __mram_ptr uint8_t* offsetsBegin = builder->curBlockPtr + varLenBlockDesc->blockDescBase.totalSize - offsetsLen;
    mram_write_unaligned(varLenBlockDesc->offsets, offsetsBegin, offsetsLen);
    uint32_t paddingOffsetsLen = ROUND_UP_TO_8(offsetsLen);
    // update total size
    varLenBlockDesc->blockDescBase.totalSize += paddingOffsetsLen - offsetsLen;
    builder->bufferDesc.header.totalSize += paddingOffsetsLen - offsetsLen;
    builder->curBlockOffset += varLenBlockDesc->blockDescBase.totalSize;
    buddy_free(varLenBlockDesc->offsets);
    // flush block header
    mram_write_unaligned(varLenBlockDesc, builder->curBlockPtr, sizeof(BlockDescriptorBase));
  } else {
    FixedLenBlockDescriptor* fixedLenBlockDesc = &builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx++];
    builder->curBlockOffset += fixedLenBlockDesc->blockDescBase.totalSize;
    // flush block header
    mram_write_unaligned(fixedLenBlockDesc, builder->curBlockPtr, sizeof(BlockDescriptorBase));
  }
  builder->curBlockPtr = replyBuffer + builder->curBlockOffset;
  // flush buffer header
  mram_write_unaligned(&builder->bufferDesc.header, replyBuffer, sizeof(DpuBufferHeader));
}

size_t BufferBuilderFinish(BufferBuilder *builder)
{
  // flush offsets of buffer
  size_t size = builder->bufferDesc.header.totalSize;
  uint32_t offsetsLen = builder->bufferDesc.header.blockCnt * sizeof(Offset);
  __mram_ptr uint8_t* offsetsBegin = replyBuffer + size - offsetsLen;
  mram_write_unaligned(builder->bufferDesc.offsets, offsetsBegin, offsetsLen);
  size = ROUND_UP_TO_8(size);
  builder->bufferDesc.header.totalSize = size;
  // flush buffer header
  mram_write_unaligned(&builder->bufferDesc.header, replyBuffer, sizeof(DpuBufferHeader));
  return size;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{
  switch(task->taskType) {
  case GET_OR_INSERT_RESP: {
    GetOrInsertResp *resp = (GetOrInsertResp*)task;
    // record the offset and task count++
    VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc.varLenBlockDescs[builder->varLenBlockIdx];
    varLenBlockDesc->offsets[varLenBlockDesc->blockDescBase.taskCount++] = builder->curTaskOffset;
    uint32_t taskSize = GetFixedLenTaskSize(task);
    mram_write_unaligned(resp, builder->curBlockPtr + builder->curTaskOffset, taskSize);
    builder->curTaskOffset += taskSize;
    // update total size
    varLenBlockDesc->blockDescBase.totalSize += taskSize + sizeof(Offset);
    builder->bufferDesc.header.totalSize += taskSize + sizeof(Offset);
    break;
  }
  default:
    // TODO impl other tasks
    Unimplemented("other tasks to be impl!\n");
    break;
  }
}
