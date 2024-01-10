#include <mram_unaligned.h>
#include <alloc.h>
#include "task_executor.h"
#include "dpu/dpu_buffer_builder.h"
#include "dpu/index_req.h"

extern uint8_t __mram_noinit replyBuffer[BUFFER_LEN];
extern uint8_t __mram_noinit receiveBuffer[BUFFER_LEN];

void InitTaskOffsets(BufferDecoder *decoder) {
  decoder->curTaskOffsetPtr = NULL;
  if (decoder->isCurVarLenBlock) {
    if ((decoder->blockHeader).taskCount > decoder->tskOffsetsLen) {
      decoder->tskOffsets = mem_alloc((decoder->blockHeader).taskCount * sizeof(Offset));
      decoder->tskOffsetsLen = (decoder->blockHeader).taskCount;
    }
    uint32_t offsetsLenTask = ROUND_UP_TO_8((decoder->blockHeader).taskCount * sizeof(Offset));
    decoder->curTaskOffsetPtr = (__mram_ptr Offset*)(decoder->curBlockPtr + (decoder->blockHeader).totalSize - offsetsLenTask);
    mram_read_unaligned(decoder->curTaskOffsetPtr, decoder->tskOffsets, offsetsLenTask);
  }
}

void GetNextBlockHeader(BufferDecoder *decoder) {
  if (decoder->blockIdx % OFFSETS_BUF_CAP == 0) { // Fetch new offsets buffer
    uint32_t offset = decoder->blockIdx / OFFSETS_BUF_CAP * OFFSETS_BUF_CAP;
    mram_read_unaligned(decoder->curBlockOffsetPtr + offset, decoder->blkOffsets, sizeof(Offset) * OFFSETS_BUF_CAP);
  }
  Offset blockOffset = decoder->blkOffsets[decoder->blockIdx % OFFSETS_BUF_CAP];
  decoder->curBlockPtr = decoder->bufPtr + blockOffset;
  decoder->blockIdx++;
  mram_read_unaligned(decoder->curBlockPtr, &(decoder->blockHeader), sizeof(BlockDescriptorBase));
}

DecoderStateT InitNextBlock(BufferDecoder *decoder) {
  // blockIdx points to the next block to get
  if (decoder->blockIdx >= (decoder->bufHeader).blockCnt) {
    return NO_MORE_BLOCK;
  }
  GetNextBlockHeader(decoder);

  decoder->curTaskPtr = decoder->curBlockPtr + sizeof(BlockDescriptorBase);
  decoder->isCurVarLenBlock = IsVarLenTask((decoder->blockHeader).taskType);
  if (!decoder->isCurVarLenBlock) {
    __dma_aligned uint8_t taskBuf[TASK_HEADER_LEN];
    Task *task = (Task*)taskBuf;
    mram_read_unaligned(decoder->curBlockPtr + BLOCK_HEAD_LEN, task, TASK_HEADER_LEN);
    decoder->taskLen = GetTaskLen(task);
  }
  InitTaskOffsets(decoder);
  return NEW_BLOCK;
}

void BufferDecoderInit(BufferDecoder *decoder) {
  decoder->blockIdx = 0;
  decoder->taskIdx = 0;
  decoder->bufPtr = receiveBuffer;
  decoder->tskOffsetsLen = OFFSETS_CAP;
  decoder->tskOffsets = mem_alloc(OFFSETS_CAP * sizeof(Offset));
  mram_read_unaligned(decoder->bufPtr, &(decoder->bufHeader), sizeof(CpuBufferHeader));
  decoder->curTaskOffsetPtr = NULL;
  decoder->isCurVarLenBlock = false;
  if ((decoder->bufHeader).blockCnt == 0) { // empty buffer
      decoder->curBlockPtr = NULL;
      decoder->curTaskPtr = NULL;
      decoder->curBlockOffsetPtr = NULL;
  } else { // point to the 1st task of the 1st block, don't mind if it's empty
    uint32_t offsetsLenBlock = (decoder->bufHeader).blockCnt * sizeof(Offset);
    offsetsLenBlock = ALIGN_TO(offsetsLenBlock, 8);
    decoder->curBlockOffsetPtr = (__mram_ptr Offset*)(decoder->bufPtr + (decoder->bufHeader).totalSize - offsetsLenBlock);

    // InitNextBlock(decoder);
  }
}

void ReadTask(__mram_ptr uint8_t* tskPtr, Task *task) {
  mram_read_unaligned(tskPtr, task, TASK_HEADER_LEN);
  uint32_t taskLen = ROUND_UP_TO_8(GetTaskLen(task));
  assert(taskLen < TASK_MAX_LEN && "Task size overflow");
  if (taskLen > TASK_HEADER_LEN) {
    mram_read_unaligned(tskPtr, task, taskLen);
  }
}

DecoderStateT GetKthTask(BufferDecoder *decoder, uint32_t idxK, Task *task) {
  if (idxK >= (decoder->blockHeader).taskCount) {
    return NO_MORE_TASK;
  }

  if (decoder->isCurVarLenBlock) {
    ReadTask(decoder->curBlockPtr + decoder->tskOffsets[idxK], task);
  } else {
    __mram_ptr uint8_t * base = decoder->curBlockPtr + BLOCK_HEAD_LEN;
    ReadTask(base + idxK * decoder->taskLen, task);
  }
  return NEW_TASK;
}

// DecoderStateT GetNextTask (BufferDecoder *decoder, Task *task) {
//   DecoderStateT state = NEW_TASK;
//   if (decoder->taskIdx >= (decoder->blockHeader).taskCount) {
//     if (decoder->blockIdx >= (decoder->bufHeader).blockCnt) { // all tasks have been decoded
//       state = NO_MORE_TASK;
//       return state;
//     } else { // move to next non-empty block
//       decoder->taskIdx = 0;
//       (decoder->blockHeader).taskCount = 0;
//       state = NEW_BLOCK;
//       while ((decoder->blockHeader).taskCount == 0) {
//         if (decoder->blockIdx >= (decoder->bufHeader).blockCnt) {
//           state = NO_MORE_TASK;
//           return state;
//         }
//         GetNextBlockHeader(decoder);
//       }
//       decoder->curTaskPtr = decoder->curBlockPtr + BLOCK_HEAD_LEN;
//       decoder->isCurVarLenBlock = IsVarLenTask((decoder->blockHeader).taskType);
//       InitTaskOffsets(decoder);
//     }
//   }

//   // get the next task from the current block
//   ReadTask(decoder->curTaskPtr, task);
//   decoder->taskIdx++;
//   if (decoder->isCurVarLenBlock) { // Var-length task
//     Offset taskOffset = decoder->tskOffsets[decoder->taskIdx];
//     decoder->curTaskPtr = decoder->curBlockPtr + taskOffset;
//   } else { // Fix-length task
//     decoder->curTaskPtr += decoder->taskLen;
//   }
//   return state;
// }

// void GetOrInsertFake (HashTableId id, char *key, uint32_t keyLen, TupleIdT tupleId, HashTableQueryReplyT *reply) {
//   printf("Warning: this is GetOrInsertFake!\n");
//   printf("hashTableId=%d, keyLen=%d, key=", id, keyLen);
//   for (int i=0; i < keyLen; i++) {
//     printf("%c", (unsigned char)key[i]);
//   }
//   printf(" (TupleIdT){.tableId = %d,\t .tupleAddr = 0x%lx}\n", tupleId.tableId, tupleId.tupleAddr);

//   // To fill a fake reply
//   reply->type = HashAddr;
//   (reply->value).hashAddr.rPtr.dpuId = 0xaaaa;
//   (reply->value).hashAddr.rPtr.dpuAddr = 0xffff;
// }

// void ExecuteTaskThenAppend (BufferBuilder *builder, Task *task) {
//   switch(task->taskType) {
//   case GET_OR_INSERT_REQ: {
//     GetOrInsertReq *req = (GetOrInsertReq*)task;
//     __dma_aligned HashTableQueryReplyT reply_buffer;
//     /* primary_index_dpu *pid = IndexCheck(req->hashTableId); */
//     /* IndexGetOrInsertReq(pid, (char*)(req->ptr), req->len, req->tid, &reply_buffer); */
//     GetOrInsertFake(req->hashTableId, (char*)(req->ptr), req->len, req->tid, &reply_buffer);
//     GetOrInsertResp resp = {
//       .base = {.taskType = GET_OR_INSERT_RESP},
//       .tupleIdOrMaxLinkAddr = reply_buffer
//     };
//     BufferBuilderAppendTask(builder, (Task*)&resp);
//     break;
//   }
//   default:
//     Unimplemented("Other tasks need to be supported.\n");
//   }
// }


// void DpuMainLoop () {
//   __dma_aligned BufferDecoder decoder;
//   BufferDecoderInit(&decoder);
//   __dma_aligned BufferBuilder builder;
//   BufferBuilderInit(&builder);
//   if (decoder.bufHeader.blockCnt > 0) {
//     BufferBuilderBeginBlock(&builder, RespTaskType(decoder.blockHeader.taskType));
//   }

//   __dma_aligned uint8_t taskBuf[TASK_MAX_LEN];
//   Task *task = (Task*)taskBuf;
//   while (true) {
//     DecoderStateT state = GetNextTask(&decoder, task);
//     if (NO_MORE_TASK == state) { // All tasks consumed
//       break;
//     }
//     if (NEW_BLOCK == state) {
//       BufferBuilderEndBlock(&builder);
//       BufferBuilderBeginBlock(&builder, RespTaskType(decoder.blockHeader.taskType));
//     }
//     ExecuteTaskThenAppend(&builder, task);
//   }

//   if (decoder.bufHeader.blockCnt > 0) {
//     BufferBuilderEndBlock(&builder);
//   }
//   BufferBuilderFinish(&builder);
// }