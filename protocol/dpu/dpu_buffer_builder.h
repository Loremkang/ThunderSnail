#ifndef DPU_BUFFER_BUILDER_H
#define DPU_BUFFER_BUILDER_H

#include "../protocol.h"

#define BUDDY_LEN 4096 // save builder and descriptors
#define BUFFER_STATE_OK 0

// reply buffer
uint8_t __mram_noinit replyBuffer[BUFFER_LEN];

typedef struct {
  DpuToCpuBufferDescriptor bufferDesc;
  __mram_ptr uint8_t *curBlockPtr;
  Offset curBlockOffset;
  uint8_t *curTaskPtr;
  Offset curTaskOffset;
  uint8_t varLenBlockIdx;
  uint8_t fixedLenBlockIdx;
  bool isCurVarLenBlock;
} BufferBuilder;

void BufferBuilderInit(BufferBuilder *builder);

void BufferBuilderBeginBlock(BufferBuilder *builder, uint8_t taskType);

void BufferBuilderEndBlock(BufferBuilder *builder);

size_t BufferBuilderFinish(BufferBuilder *builder);

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task);

#endif
