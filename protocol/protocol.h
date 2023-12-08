#pragma once
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>
#include "../common_base_struct/common_base_struct.h"

// define task names, request and response
#define SET_DPU_ID_REQ 0
#define CREATE_INDEX_REQ 1
#define GET_OR_INSERT_REQ 2
#define GET_POINTER_REQ 3
#define UPDATE_POINTER_REQ 4
#define GET_MAX_LINK_SIZE_REQ 5
#define FETCH_MAX_LINK_REQ 6
#define MERGE_MAX_LINK_REQ 7
#define NEW_MAX_LINK_REQ 14

#define EMPTY_RESP 128
#define GET_OR_INSERT_RESP 129
#define GET_POINTER_RESP 130
#define UPDATE_POINTER_RESP 131
#define GET_MAX_LINK_SIZE_RESP 132
#define FETCH_MAX_LINK_RESP 133
#define MERGE_MAX_LINK_RESP 134
#define NEW_MAX_LINK_RESP 135

// align to 8
#define NUM_FIXED_LEN_BLOCK_INPUT 4
#define NUM_VAR_LEN_BLOCK_INPUT 4
#define NUM_FIXED_LEN_BLOCK_OUTPUT 4
#define NUM_VAR_LEN_BLOCK_OUTPUT 4

#define CPU_BUFFER_HEAD_LEN 8 // |epochNumber blockCnt totalSize|
#define DPU_BUFFER_HEAD_LEN 8 // |bufferState blockCnt totalSize|
#define BLOCK_HEAD_LEN sizeof(BlockDescriptorBase)
#define BATCH_SIZE 320
#define NUM_BLOCKS 8

#define BUFFER_LEN 65535
#define ROUND_UP_TO_8(x) (((x)+7) &~7) // to align key len to 8

#define ALIGN_TO( sizeToAlign, PowerOfTwo )                         \
        (((sizeToAlign) + (PowerOfTwo) - 1) & ~((PowerOfTwo) - 1))

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

#include "../dpu/task.h"

bool IsVarLenTask(uint8_t taskType);

uint16_t GetFixedLenTaskSize(void *task);

#endif
