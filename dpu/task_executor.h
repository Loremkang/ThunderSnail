#ifndef TASK_EXECUTOR_H
#define TASK_EXECUTOR_H

#include "protocol.h"

// Make sure TASK_HEADER_LEN is long enough to calculate any task's length
#define TASK_HEADER_LEN 128
// Could a task longer than this? But mram_read only supports reads <= 2048 Bytes
#define TASK_MAX_LEN 512
#define OFFSETS_BUF_CAP 2
#define OFFSETS_CAP 256

typedef enum {
  NO_MORE_TASK = 0,
  NO_MORE_BLOCK = 0,
  NEW_TASK = 1,
  NEW_BLOCK = 2
} DecoderStateT;

typedef struct {
  CpuBufferHeader bufHeader;
  BlockDescriptorBase blockHeader;
  Offset blkOffsets[OFFSETS_BUF_CAP];
  Offset* tskOffsets;
  uint16_t blockIdx;
  uint16_t taskIdx;
  __mram_ptr uint8_t *bufPtr;
  __mram_ptr uint8_t *curBlockPtr;
  __mram_ptr uint8_t *curTaskPtr;
  __mram_ptr Offset *curBlockOffsetPtr;
  __mram_ptr Offset *curTaskOffsetPtr;
  uint16_t remainingTaskCnt;
  uint32_t taskLen;
  uint32_t tskOffsetsLen;
  bool isCurVarLenBlock;
} BufferDecoder;

void BufferDecoderInit(BufferDecoder *decoder);
void InitTaskOffsets(BufferDecoder *decoder);
DecoderStateT InitNextBlock(BufferDecoder *decoder);
DecoderStateT GetKthTask(BufferDecoder *decoder, uint32_t idxK, Task *task);
// void DpuMainLoop();
#endif