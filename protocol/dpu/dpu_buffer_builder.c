#include "dpu_buffer_builder.h"
#include <alloc.h>
#include <stdlib.h>
#include <string.h>
#include <mram.h>
#include <mram_unaligned.h>

// reply buffer
__mram_noinit uint8_t replyBuffer[BUFFER_LEN];
__mram_noinit uint8_t receiveBuffer[BUFFER_LEN];
__mram_noinit Offset offsetsBuffMRAM[TASK_COUNT_PER_BLOCK];
__dma_aligned __host Offset varlenBufferOffsets[OFFSETS_CAP];

void BufferBuilderInit(BufferBuilder *builder)
{
  // buddy_init(BUDDY_LEN);
  // alloc the whole buffer
  builder->bufferDesc.header.bufferState = BUFFER_STATE_OK;
  builder->bufferDesc.header.blockCnt = 0;
  builder->bufferDesc.header.totalSize = DPU_BUFFER_HEAD_LEN;
  // need clear the reply buffer?
  memset(replyBuffer, 0, BUFFER_LEN);

  builder->curBlockPtr = replyBuffer + DPU_BUFFER_HEAD_LEN;
  builder->curBlockOffset = DPU_BUFFER_HEAD_LEN;

  builder->varLenBlockIdx = 0;
  builder->fixedLenBlockIdx = 0;
  builder->curTaskOffset = 0;

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
    varLenBlockDesc->offsets = varlenBufferOffsets;
  } else {
    builder->isCurVarLenBlock = false;
    builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx].blockDescBase = (BlockDescriptorBase) {
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
    uint16_t taskCnt = varLenBlockDesc->blockDescBase.taskCount;
    uint32_t offsetsLen = taskCnt * sizeof(Offset);
    __mram_ptr uint8_t* offsetsBegin = builder->curBlockPtr + varLenBlockDesc->blockDescBase.totalSize - offsetsLen;
    offsetsLen = (taskCnt % OFFSETS_CAP) * sizeof(Offset);
    uint32_t paddingOffsetsLen = ROUND_UP_TO_8(offsetsLen);

    if (offsetsLen > 0) {
      mram_write_large(varLenBlockDesc->offsets,
                       offsetsBegin + sizeof(Offset) * (taskCnt - taskCnt % OFFSETS_CAP ),
                       paddingOffsetsLen);
    }

    if (taskCnt >= OFFSETS_CAP) {
      // flush offsets from MRAM to MRAM
      for (int i=0; i < taskCnt / OFFSETS_CAP; i++){
        mram_read_large(&offsetsBuffMRAM[i * OFFSETS_CAP], varLenBlockDesc->offsets, sizeof(Offset) * OFFSETS_CAP);
        mram_write_large(varLenBlockDesc->offsets, offsetsBegin + (i * OFFSETS_CAP * sizeof(Offset)), sizeof(Offset) * OFFSETS_CAP);
      }
    }

    // update total size
    varLenBlockDesc->blockDescBase.totalSize += paddingOffsetsLen - offsetsLen;
    builder->bufferDesc.header.totalSize += paddingOffsetsLen - offsetsLen;
    builder->curBlockOffset += varLenBlockDesc->blockDescBase.totalSize;

    // flush block header
    mram_write_small(varLenBlockDesc, builder->curBlockPtr, sizeof(BlockDescriptorBase));
  } else {
    FixedLenBlockDescriptor* fixedLenBlockDesc = &builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx++];
    builder->curBlockOffset += fixedLenBlockDesc->blockDescBase.totalSize;
    // flush block header
    mram_write_small(fixedLenBlockDesc, builder->curBlockPtr, sizeof(BlockDescriptorBase));
  }
  builder->curBlockPtr = replyBuffer + builder->curBlockOffset;
  // flush buffer header
  mram_write_small(&builder->bufferDesc.header, replyBuffer, sizeof(DpuBufferHeader));
}

size_t BufferBuilderFinish(BufferBuilder *builder)
{
  // flush offsets of buffer
  size_t size = builder->bufferDesc.header.totalSize;
  uint32_t offsetsLen = builder->bufferDesc.header.blockCnt * sizeof(Offset);
  __mram_ptr uint8_t* offsetsBegin = replyBuffer + size - offsetsLen;
  uint32_t paddingOffsetsLen = ROUND_UP_TO_8(offsetsLen);
  mram_write_small(builder->bufferDesc.offsets, offsetsBegin, paddingOffsetsLen);
  size = ROUND_UP_TO_8(size);
  builder->bufferDesc.header.totalSize = size;
  // flush buffer header
  mram_write_small(&builder->bufferDesc.header, replyBuffer, sizeof(DpuBufferHeader));
  return size;
}

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task)
{
  switch(task->taskType) {
  case FETCH_MAX_LINK_RESP:
    {
      // record the offset and task count++
      VarLenBlockDescriptor* varLenBlockDesc = &builder->bufferDesc.varLenBlockDescs[builder->varLenBlockIdx];

      varLenBlockDesc->offsets[varLenBlockDesc->blockDescBase.taskCount % OFFSETS_CAP] = builder->curTaskOffset;
      varLenBlockDesc->blockDescBase.taskCount++;
      //ArrayOverflowCheck(varLenBlockDesc->blockDescBase.taskCount <= TASK_COUNT_PER_BLOCK);
      if (varLenBlockDesc->blockDescBase.taskCount % OFFSETS_CAP == 0) {
        mram_write_large(varLenBlockDesc->offsets,
                         &offsetsBuffMRAM[(varLenBlockDesc->blockDescBase.taskCount / OFFSETS_CAP - 1) * OFFSETS_CAP],
                         sizeof(Offset) * OFFSETS_CAP);
      }

      uint32_t taskSize = GetTaskLen(task);
      mram_write_small(task, builder->curBlockPtr + builder->curTaskOffset, taskSize);
      builder->curTaskOffset += taskSize;
      // updata total size
      varLenBlockDesc->blockDescBase.totalSize += taskSize + sizeof(Offset);
      builder->bufferDesc.header.totalSize += taskSize + sizeof(Offset);
      ArrayOverflowCheck(varLenBlockDesc->blockDescBase.totalSize < BUFFER_LEN);
      break;
    }
  case GET_OR_INSERT_RESP:
  case GET_POINTER_RESP:
  case GET_MAX_LINK_SIZE_RESP:
  case NEW_MAX_LINK_RESP:
  case GET_VALID_MAXLINK_COUNT_RESP:
    {
    // record the offset and task count++
    FixedLenBlockDescriptor* fixedLenBlockDesc = &builder->bufferDesc.fixedLenBlockDescs[builder->fixedLenBlockIdx];
    fixedLenBlockDesc->blockDescBase.taskCount++;
    uint32_t taskSize = GetTaskLen(task);
    mram_write_small(task, builder->curBlockPtr + builder->curTaskOffset, taskSize);
    builder->curTaskOffset += taskSize;
    // update total size
    fixedLenBlockDesc->blockDescBase.totalSize += taskSize;
    builder->bufferDesc.header.totalSize += taskSize;
    break;
  }
  default:
    // TODO impl other tasks
    Unimplemented("other tasks to be impl!\n");
    break;
  }
}
