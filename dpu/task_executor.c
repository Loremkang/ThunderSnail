#include <mram_unaligned.h>
#include "task_executor.h"
#include "../protocol/dpu/dpu_buffer_builder.h"
#include "../dpu/index_req.h"

extern uint8_t __mram_noinit replyBuffer[BUFFER_LEN];
extern uint8_t __mram_noinit receiveBuffer[BUFFER_LEN];

void BufferDecoderInit(BufferDecoder *decoder, __mram_ptr uint8_t *buffer) {
  decoder->blockIdx = 0;
  decoder->taskIdx = 0;
  decoder->bufHeader = *((CpuBufferHeader *)buffer); // TODO: optimize
  decoder->bufPtr = buffer;
  decoder->curTaskOffsetPtr = NULL;
  decoder->isCurVarLenBlock = false;
  if ((decoder->bufHeader).blockCnt == 0) { // empty buffer
      decoder->curBlockPtr = NULL;
      decoder->curTaskPtr = NULL;
      decoder->curBlockOffsetPtr = NULL;
  } else { // point to the 1st task of the 1st block, don't mind if it's empty
    decoder->curBlockPtr = buffer + CPU_BUFFER_HEAD_LEN;
    decoder->blockHeader = *((BlockDescriptorBase *)(decoder->curBlockPtr)); // TODO: optimize
    decoder->curTaskPtr = decoder->curBlockPtr + BLOCK_HEAD_LEN;
    uint32_t offsetsLenBlock = (decoder->bufHeader).blockCnt * sizeof(Offset);
    decoder->curBlockOffsetPtr = (__mram_ptr Offset*)(decoder->bufPtr + (decoder->bufHeader).totalSize - offsetsLenBlock);
    decoder->isCurVarLenBlock = IsVarLenTask((decoder->blockHeader).taskType);
    if (decoder->isCurVarLenBlock) {
      uint32_t offsetsLenTask = (decoder->blockHeader).taskCount * sizeof(Offset);
      decoder->curTaskOffsetPtr = (__mram_ptr Offset*)(decoder->curBlockPtr + (decoder->blockHeader).totalSize - offsetsLenTask);
    }
  }
}

GetTaskStateT GetNextTask (BufferDecoder *decoder, Task *task) {
  GetTaskStateT state = NEW_TASK;
  if (decoder->taskIdx >= (decoder->blockHeader).taskCount) {
    if (decoder->blockIdx >= (decoder->bufHeader).blockCnt) { // all tasks have been decoded
      task = NULL;
      state = NO_MORE_TASK;
      return state;
    } else { // move to next unempty block
      decoder->taskIdx = 0;
      (decoder->blockHeader).taskCount = 0;
      state = NEW_BLOCK;
      while ((decoder->blockHeader).taskCount == 0) {
        if (decoder->blockIdx >= (decoder->bufHeader).blockCnt) {
          task = NULL;
          state = NO_MORE_TASK;
          return state;
        }
        decoder->curBlockPtr = decoder->bufPtr + *(decoder->curBlockOffsetPtr);
        decoder->curBlockOffsetPtr++;
        decoder->blockIdx++;
        decoder->blockHeader = *((BlockDescriptorBase *)(decoder->curBlockPtr));
      }
      decoder->curTaskPtr = decoder->curBlockPtr + BLOCK_HEAD_LEN;
      decoder->isCurVarLenBlock = IsVarLenTask((decoder->blockHeader).taskType);
      decoder->curTaskOffsetPtr = NULL;
      if (decoder->isCurVarLenBlock) {
        uint32_t offsetsLenTask = (decoder->blockHeader).taskCount * sizeof(Offset);
        decoder->curTaskOffsetPtr = (__mram_ptr Offset*)(decoder->curBlockPtr + (decoder->blockHeader).totalSize - offsetsLenTask);
      }
    }
  }

  // get the next task from the current block
  mram_read_unaligned(decoder->curTaskPtr, task, TASK_HEADER_LEN);
  uint32_t taskLen = GetFixedLenTaskSize(task);
  assert(taskLen < TASK_MAX_LEN && "Task size overflow");
  if (taskLen > TASK_HEADER_LEN) {
    mram_read_unaligned(decoder->curTaskPtr, task, taskLen);
  }
  if (decoder->isCurVarLenBlock) { // Var-length task
    decoder->curTaskPtr = decoder->curBlockPtr + *(decoder->curTaskOffsetPtr); // FIX-ME: should self-inc first then take the offset?
    decoder->curTaskOffsetPtr++;
  } else { // Fix-length task
    decoder->curTaskPtr += taskLen;
  }
  decoder->taskIdx++;
  return state;
}

void ExecuteTask (BufferBuilder *builder, Task *task) {
  switch(task->taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    __dma_aligned HashTableQueryReplyT reply_buffer;
    primary_index_dpu *pid = IndexCheck(req->hashTableId);
    IndexGetOrInsertReq(pid, (char*)(req->ptr), req->len, req->tid, &reply_buffer);
    GetOrInsertResp resp = {
      .base = {.taskType = GET_OR_INSERT_RESP},
      .tupleIdOrMaxLinkAddr = reply_buffer
    };
    BufferBuilderAppendTask(builder, (Task*)&resp);
    break;
  }
  default:
    Unimplemented("Other tasks need to be supported.\n");
  }
}

void DpuMainLoop () {
  BufferDecoder decoder;
  BufferDecoderInit(&decoder, receiveBuffer);

  BufferBuilder builder;
  BufferBuilderInit(&builder);
  if (decoder.bufHeader.blockCnt > 0) {
    BufferBuilderBeginBlock(&builder, decoder.blockHeader.taskType);
  }

  __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
  Task *task = (Task*)&taskBuf;
  while (true) {
    GetTaskStateT state = GetNextTask(&decoder, task);
    if (NO_MORE_TASK == state) { // All tasks consumed
      break;
    }
    if (NEW_BLOCK == state) {
      BufferBuilderEndBlock(&builder);
      BufferBuilderBeginBlock(&builder, decoder.blockHeader.taskType);
    }
    ExecuteTask(&builder, task);
  }

  BufferBuilderEndBlock(&builder);
  BufferBuilderFinish(&builder);
}