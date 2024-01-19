#ifndef CPU_BUFFER_BUILDER_H
#define CPU_BUFFER_BUILDER_H

#include "protocol.h"
#include "requests.h"

typedef enum {
  DPU_STATE_FREE,
  DPU_STATE_INITIALIZED,
  DPU_STATE_BLOCK_OPEN,
  DPU_STATE_BLOCK_CLOSED,
  DPU_STATE_CLOSED,
  DPU_STATE_RUNNING,
  DPU_STATE_FINISHED
} dpu_state;

typedef struct {
  CpuToDpuBufferDescriptor* bufferDesc;
  uint8_t* buffer;
  uint8_t* curBlockPtr;
  Offset* varlenBlockOffsetsBuffer;
  Offset curBlockOffset;
  uint8_t* curTaskPtr;
  Offset curTaskOffset;
  uint8_t varLenBlockIdx;
  uint8_t fixedLenBlockIdx;
  bool isCurVarLenBlock;
  dpu_state state;
  // int dpuId;
} BufferBuilder;

void BufferBuilderInit(BufferBuilder* builder,
                       CpuToDpuBufferDescriptor* bufferDesc, uint8_t* ioBuffer,
                       Offset* offsetBuffer, Offset* varlenBlockOffsetBuffer);

void BufferBuilderBeginBlock(BufferBuilder* builder, uint8_t taskType);

void BufferBuilderEndBlock(BufferBuilder* builder);

uint8_t* BufferBuilderFinish(BufferBuilder* builder, size_t* size);

uint8_t* BufferBuilderAppendPlaceHolder(BufferBuilder* builder,
                                        uint8_t taskType, size_t size);
void BufferBuilderAppendTask(BufferBuilder* builder, Task* task);

#endif
