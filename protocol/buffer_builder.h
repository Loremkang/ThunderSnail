#ifndef BUFFER_BUILDER_H
#define BUFFER_BUILDER_H

#include "protocol.h"

#define BUFFER_LEN 65535

typedef struct {
  CpuToDpuBufferDescriptor *bufferDesc;
  uint8_t *buffer;
  uint8_t *curPtr;
  Offset curOffset;
  uint8_t varLenBlockIdx;
  uint8_t fixedLenBlockIdx;
  uint8_t totalBlocks;
  uint16_t totalTasks;
  bool isCurVarLenBlock;
} BufferBuilder;

void BufferBuilderInit(BufferBuilder *builder, CpuToDpuBufferDescriptor *bufferDesc);

void BufferBuilderBeginBlock(BufferBuilder *builder, uint8_t taskType);

void BufferBuilderEndBlock(BufferBuilder *builder);

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size);

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task);

#endif
