#ifndef CPU_BUFFER_BUILDER_H
#define CPU_BUFFER_BUILDER_H

#include "../protocol.h"

typedef struct {
  CpuToDpuBufferDescriptor *bufferDesc;
  uint8_t *buffer;
  uint8_t *curBlockPtr;
  Offset curBlockOffset;
  uint8_t *curTaskPtr;
  Offset curTaskOffset;
  uint8_t varLenBlockIdx;
  uint8_t fixedLenBlockIdx;
  bool isCurVarLenBlock;
} BufferBuilder;

void BufferBuilderInit(BufferBuilder *builder, CpuToDpuBufferDescriptor *bufferDesc);

void BufferBuilderBeginBlock(BufferBuilder *builder, uint8_t taskType);

void BufferBuilderEndBlock(BufferBuilder *builder);

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size);

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task);

#endif
