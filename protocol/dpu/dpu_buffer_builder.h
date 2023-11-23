#ifndef DPU_BUFFER_BUILDER_H
#define DPU_BUFFER_BUILDER_H

#include "../protocol.h"

#define BUDDY_LEN 1024 // save builder and descriptors
#define NUM_OF_TASKS 128
#define BUFFER_STATE_OK 0

// reply buffer
uint8_t __mram_noinit replyBuffer[BUFFER_LEN];

typedef struct {
  DpuToCpuBufferDescriptor bufferDesc;
  uint8_t *curBlockPtr;
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

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size);

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task);

#endif
