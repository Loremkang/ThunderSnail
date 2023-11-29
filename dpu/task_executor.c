#include <mram_unaligned.h>
#include "task_executor.h"
#include "../protocol/dpu/dpu_buffer_builder.h"
#include "../dpu/index_req.h"

extern uint8_t __mram_noinit replyBuffer[BUFFER_LEN];
extern uint8_t __mram_noinit receiveBuffer[BUFFER_LEN];

void BufferDecoderInit(BufferDecoder *decoder) {
  decoder->blockIdx = 0;
  decoder->taskIdx = 0;
  decoder->bufPtr = receiveBuffer;
  mram_read_unaligned(decoder->bufPtr, &(decoder->bufHeader), sizeof(CpuBufferHeader));
  decoder->curTaskOffsetPtr = NULL;
  decoder->isCurVarLenBlock = false;
  if ((decoder->bufHeader).blockCnt == 0) { // empty buffer
      decoder->curBlockPtr = NULL;
      decoder->curTaskPtr = NULL;
      decoder->curBlockOffsetPtr = NULL;
  } else { // point to the 1st task of the 1st block, don't mind if it's empty
    decoder->blockIdx++;
    decoder->curBlockPtr = decoder->bufPtr + sizeof(CpuBufferHeader);
    mram_read_unaligned(decoder->curBlockPtr, &(decoder->blockHeader), sizeof(BlockDescriptorBase));
    decoder->curTaskPtr = decoder->curBlockPtr + sizeof(BlockDescriptorBase);
    uint32_t offsetsLenBlock = (decoder->bufHeader).blockCnt * sizeof(Offset);
    offsetsLenBlock = ALIGN_TO(offsetsLenBlock, 8);
    decoder->curBlockOffsetPtr = (__mram_ptr Offset*)(decoder->bufPtr + (decoder->bufHeader).totalSize - offsetsLenBlock);
    mram_read_unaligned(decoder->curBlockOffsetPtr, decoder->blkOffsets, sizeof(Offset) * OFFSETS_BUF_CAP);
    decoder->isCurVarLenBlock = IsVarLenTask((decoder->blockHeader).taskType);
    if (decoder->isCurVarLenBlock) {
      uint32_t offsetsLenTask = (decoder->blockHeader).taskCount * sizeof(Offset);
      offsetsLenTask = ALIGN_TO(offsetsLenTask, 8);
      decoder->curTaskOffsetPtr = (__mram_ptr Offset*)(decoder->curBlockPtr + (decoder->blockHeader).totalSize - offsetsLenTask);
      mram_read_unaligned(decoder->curTaskOffsetPtr, decoder->tskOffsets, sizeof(Offset) * OFFSETS_BUF_CAP);
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
    } else { // move to next non-empty block
      decoder->taskIdx = 0;
      (decoder->blockHeader).taskCount = 0;
      state = NEW_BLOCK;
      while ((decoder->blockHeader).taskCount == 0) {
        if (decoder->blockIdx >= (decoder->bufHeader).blockCnt) {
          task = NULL;
          state = NO_MORE_TASK;
          return state;
        }
        if (decoder->blockIdx % OFFSETS_BUF_CAP == 0) { // Fetch new offsets buffer
          uint32_t offset = decoder->blockIdx / OFFSETS_BUF_CAP * OFFSETS_BUF_CAP;
          mram_read_unaligned(decoder->curBlockOffsetPtr + offset, decoder->blkOffsets, sizeof(Offset) * OFFSETS_BUF_CAP);
        }
        Offset blockOffset = decoder->blkOffsets[decoder->blockIdx % OFFSETS_BUF_CAP];
        decoder->curBlockPtr = decoder->bufPtr + blockOffset;
        decoder->blockIdx++;
        mram_read_unaligned(decoder->curBlockPtr, &(decoder->blockHeader), sizeof(BlockDescriptorBase));
      }
      decoder->curTaskPtr = decoder->curBlockPtr + BLOCK_HEAD_LEN;
      decoder->isCurVarLenBlock = IsVarLenTask((decoder->blockHeader).taskType);
      decoder->curTaskOffsetPtr = NULL;
      if (decoder->isCurVarLenBlock) {
        uint32_t offsetsLenTask = (decoder->blockHeader).taskCount * sizeof(Offset);
        decoder->curTaskOffsetPtr = (__mram_ptr Offset*)(decoder->curBlockPtr + (decoder->blockHeader).totalSize - offsetsLenTask);
        mram_read_unaligned(decoder->curTaskOffsetPtr, decoder->tskOffsets, sizeof(Offset) * OFFSETS_BUF_CAP);
      }
    }
  }

  // get the next task from the current block
  mram_read_unaligned(decoder->curTaskPtr, task, TASK_HEADER_LEN);
  uint32_t taskLen = GetFixedLenTaskSize(task);
  taskLen = ALIGN_TO(taskLen, 8);
  assert(taskLen < TASK_MAX_LEN && "Task size overflow");
  if (taskLen > TASK_HEADER_LEN) {
    mram_read_unaligned(decoder->curTaskPtr, task, taskLen);
  }
  decoder->taskIdx++;
  if (decoder->isCurVarLenBlock) { // Var-length task
    if (decoder->taskIdx % OFFSETS_BUF_CAP == 0) { // Fetch new offsets buffer
      uint32_t offset = (decoder->taskIdx / OFFSETS_BUF_CAP) * OFFSETS_BUF_CAP;
      mram_read_unaligned(decoder->curTaskOffsetPtr + offset, decoder->tskOffsets, sizeof(Offset) * OFFSETS_BUF_CAP);
    }
    Offset taskOffset = decoder->tskOffsets[decoder->taskIdx % OFFSETS_BUF_CAP];
    decoder->curTaskPtr = decoder->curBlockPtr + taskOffset;
  } else { // Fix-length task
    decoder->curTaskPtr += taskLen;
  }
  return state;
}

void GetOrInsertFake (HashTableId id, char *key, uint32_t keyLen, TupleIdT tupleId, HashTableQueryReplyT *reply) {
  printf("Warning: this is GetOrInsertFake!\n");
  printf("hashTableId=%d, keyLen=%d, key=", id, keyLen);
  for (int i=0; i < keyLen; i++) {
    printf("%c", (unsigned char)key[i]);
  }
  printf(" (TupleIdT){.tableId = %d,\t .tupleAddr = 0x%lx}\n", tupleId.tableId, tupleId.tupleAddr);

  // To fill a fake reply
  reply->type = HashAddr;
  (reply->value).hashAddr.rPtr.dpuId = 0xaaaa;
  (reply->value).hashAddr.rPtr.dpuAddr = 0xffff;
}

void ExecuteTaskThenAppend (BufferBuilder *builder, Task *task) {
  switch(task->taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    __dma_aligned HashTableQueryReplyT reply_buffer;
    /* primary_index_dpu *pid = IndexCheck(req->hashTableId); */
    /* IndexGetOrInsertReq(pid, (char*)(req->ptr), req->len, req->tid, &reply_buffer); */
    GetOrInsertFake(req->hashTableId, (char*)(req->ptr), req->len, req->tid, &reply_buffer);
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

uint8_t RespTaskType(uint8_t taskType) {
  switch(taskType) {
  case GET_OR_INSERT_REQ:
    return GET_OR_INSERT_RESP;
  case GET_POINTER_REQ:
    return GET_POINTER_RESP;
  case UPDATE_POINTER_REQ:
    return UPDATE_POINTER_RESP;
  case GET_MAX_LINK_SIZE_REQ:
    return GET_MAX_LINK_SIZE_RESP;
  default:
    ValidValueCheck(0);
    return -1;
  }
}

void DpuMainLoop () {
  __dma_aligned BufferDecoder decoder;
  BufferDecoderInit(&decoder);
  __dma_aligned BufferBuilder builder;
  BufferBuilderInit(&builder);
  if (decoder.bufHeader.blockCnt > 0) {
    BufferBuilderBeginBlock(&builder, RespTaskType(decoder.blockHeader.taskType));
  }

  __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
  Task *task = (Task*)taskBuf;
  while (true) {
    GetTaskStateT state = GetNextTask(&decoder, task);
    if (NO_MORE_TASK == state) { // All tasks consumed
      break;
    }
    if (NEW_BLOCK == state) {
      BufferBuilderEndBlock(&builder);
      BufferBuilderBeginBlock(&builder, RespTaskType(decoder.blockHeader.taskType));
    }
    ExecuteTaskThenAppend(&builder, task);
  }

  if (decoder.bufHeader.blockCnt > 0) {
    BufferBuilderEndBlock(&builder);
  }
  BufferBuilderFinish(&builder);
}