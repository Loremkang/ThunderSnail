#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>
#include "../common_base_struct/common_base_struct.h"
#include "buffer_builder.h"

// define task names, request and response
#define GET_OR_INSERT_REQ 0
#define GET_POINTER_REQ 1
#define UPDATE_POINTER_REQ 2
#define GET_MAX_LINK_SIZE_REQ 3
#define FETCH_MAX_LINK_REQ 4
#define MERGE_MAX_LINK_REQ 5
#define GET_OR_INSERT_RESP 6
#define GET_POINTER_RESP 7
#define UPDATE_POINTER_RESP 8
#define GET_MAX_LINK_SIZE_RESP 9
#define FETCH_MAX_LINK_RESP 10
#define MERGE_MAX_LINK_RESP 11

typedef struct {
  uint8_t taskType;
  uint16_t taskCount;
  uint32_t totalSize;
  void *tasks;
} BlockDescriptorBase;

typedef struct {
  BlockDescriptorBase blockDescBase;
} FixedLenBlockDescriptor;

typedef struct {
  BlockDescriptorBase blockDescBase;
  uint32_t *offsets;
} VarLenBlockDescriptor;

typedef struct {
  uint16_t epochNumber;
  uint16_t blockCnt;
  uint16_t totalSize;
  FixedLenBlockDescriptor *fixedLenblockDescs;
  VarLenBlockDescriptor *varLenblockDescs;
  uint16_t *offsets;
} CpuToDpuBufferDescriptor;

void CreateCpuToDpuBufferForEachDPU()
{
  uint8_t *buffer;
  size_t size;
  BufferBuilder builder, *B;
  B = &builder;
  // we should known num of fixed len blocks
  uint16_t numBlocks = 2;
  uint16_t numFixedLenBlocks = 1;
  uint16_t numFixedLenTasks = 128;
  uint16_t numVarLenTasks = 256;
  int batchSize = 320;
  FixedLenBlockDescriptor *fixedLenBlockDescs = malloc(numFixedLenBlocks * sizeof(FixedLenBlockDescriptor));
  for (int i = 0; i < numFixedLenBlocks; i++) {
    fixedLenBlockDescs[i].blockDescBase = {
      .taskType = GET_OR_INSERT_REQ
      // taskCount will be calc later
    };
  }
  VarLenBlockDescriptor *varLenBlockDescs = malloc((numBlocks - numFixedLenBlocks) * sizeof(VarLenBlockDescriptor));
  for (int i = 0; i < numVarLenBlocks; i++) {
    varLenBlockDescs[i].blockDescBase = {
      .taskType = GET_OR_INSERT_REQ
    };
  }
  CpuToDpuBufferDescriptor bufferDesc = {
    .epochNumber = 8,
    .blockCnt = numBlocks
  };
  BufferBuilderInit(B, &bufferDesc);
  BufferBuilderBeginBlock(B, varLenBlockDescs);
  // input stream to get task
  for (int i = 0; i < numVarLenTasks; i++) {
    BufferBuilderAppendTask(B, task);
  }
  BufferBuilderEndBlock(B);
  buffer = BufferBuilderFinish(B, &size);
  return;
}
#endif
