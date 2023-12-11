#pragma once
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>
#include "../common_base_struct/common_base_struct.h"


typedef ALIGN8 struct {
  uint8_t taskType;
  uint16_t taskCount;
  uint32_t totalSize;
} BlockDescriptorBase;

typedef struct {
  BlockDescriptorBase blockDescBase;
} FixedLenBlockDescriptor;

typedef struct {
  BlockDescriptorBase blockDescBase;
  Offset *offsets;
} VarLenBlockDescriptor;

typedef ALIGN8 struct {
  uint8_t epochNumber;
  uint16_t blockCnt;
  uint32_t totalSize;
} CpuBufferHeader;

typedef struct {
  CpuBufferHeader header;
  FixedLenBlockDescriptor fixedLenBlockDescs[NUM_FIXED_LEN_BLOCK_INPUT];
  VarLenBlockDescriptor varLenBlockDescs[NUM_VAR_LEN_BLOCK_INPUT];
  Offset *offsets;
} CpuToDpuBufferDescriptor;

typedef ALIGN8 struct {
  uint8_t bufferState;
  uint16_t blockCnt;
  uint32_t totalSize;
} DpuBufferHeader;

typedef struct {
  DpuBufferHeader header;
  FixedLenBlockDescriptor fixedLenBlockDescs[NUM_FIXED_LEN_BLOCK_OUTPUT];
  VarLenBlockDescriptor varLenBlockDescs[NUM_VAR_LEN_BLOCK_OUTPUT];
  Offset offsets[NUM_BLOCKS];
} DpuToCpuBufferDescriptor;

#include "../common_base_struct/task.h"
#endif
