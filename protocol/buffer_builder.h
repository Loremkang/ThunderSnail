#ifndef BUFFER_BUILDER_H
#define BUFFER_BUILDER_H

#include "protocol.h"

#define BUFFER_LEN 65535

typedef uint32_t Offset; // The buffer offset

typedef struct {
  CpuToDpuBufferDescriptor *bufferDesc;
  uint8_t *buffer;
  uint8_t *curPtr;
  Offset curOffset;
  uint8_t curBlock;
} BufferBuilder;

void BufferBuilderInit(BufferBuilder *builder, CpuToDpuBufferDescriptor *bufferDesc);

void BufferBuilderBeginBlock(BufferBuilder *builder, Task *firstTask);

void BufferBuilderEndBlock(BufferBuilder *builder);

uint8_t* BufferBuilderFinish(BufferBuilder *builder, size_t *size);

void BufferBuilderAppendTask(BufferBuilder *builder, Task *task);

#endif
