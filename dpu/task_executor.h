#ifndef TASK_EXECUTOR_H
#define TASK_EXECUTOR_H

#include <seqread.h>
#include "../protocol/protocol.h"

// Make sure TASK_HEADER_LEN is long enough to calculate any task's length
#define TASK_HEADER_LEN 128
// Could a task longer than this? But mram_read only supports reads <= 2048 Bytes
#define TASK_MAX_LEN 2048

typedef enum {
  NO_MORE_TASK = 0,
  NEW_TASK = 1,
  NEW_BLOCK = 2
} GetTaskStateT;

typedef struct {
  CpuBufferHeader bufHeader;
  BlockDescriptorBase blockHeader;
  Offset* blkOffsets;
  Offset* tskOffsets;
  uint16_t blockIdx;
  uint16_t taskIdx;
  __mram_ptr uint8_t *bufPtr;
  __mram_ptr uint8_t *curBlockPtr;
  __mram_ptr uint8_t *curTaskPtr;
  __mram_ptr Offset *curBlockOffsetPtr;
  __mram_ptr Offset *curTaskOffsetPtr;
  seqreader_t blkSr;
  seqreader_t tskSr;
  seqreader_buffer_t blkCache;
  seqreader_buffer_t tskCache;
  bool isCurVarLenBlock;
} BufferDecoder;

void DpuMainLoop();
#endif