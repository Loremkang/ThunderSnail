#ifndef BUFFER_BUILDER_H
#define BUFFER_BUILDER_H

typedef struct {
  CpuToDpuBufferDescriptor *bufferDesc;
  uint8_t *buffer;
} BufferBuilder;
#endif
