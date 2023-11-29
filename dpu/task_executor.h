#ifndef TASK_EXECUTOR_H
#define TASK_EXECUTOR_H

#include "../protocol/protocol.h"

// Make sure TASK_HEADER_LEN is long enough to calculate any task's length
#define TASK_HEADER_LEN 128
// Could a task longer than this? But mram_read only supports reads <= 2048 Bytes
#define TASK_MAX_LEN 2048
#define OFFSETS_BUF_CAP 2

typedef enum {
  NO_MORE_TASK = 0,
  NEW_TASK = 1,
  NEW_BLOCK = 2
} GetTaskStateT;

typedef struct {
  CpuBufferHeader bufHeader;
  BlockDescriptorBase blockHeader;
  Offset blkOffsets[OFFSETS_BUF_CAP];
  Offset tskOffsets[OFFSETS_BUF_CAP];
  uint16_t blockIdx;
  uint16_t taskIdx;
  __mram_ptr uint8_t *bufPtr;
  __mram_ptr uint8_t *curBlockPtr;
  __mram_ptr uint8_t *curTaskPtr;
  __mram_ptr Offset *curBlockOffsetPtr;
  __mram_ptr Offset *curTaskOffsetPtr;
  bool isCurVarLenBlock;
} BufferDecoder;

void DpuMainLoop();
#endif