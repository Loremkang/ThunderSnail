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

#define NUM_FIXED_LEN_BLOCK_INPUT 3
#define NUM_VAR_LEN_BLOCK_INPUT 3 // contain key, maxlink as input parameters tasks
#define NUM_FIXED_LEN_BLOCK_OUTPUT 3
#define NUM_VAR_LEN_BLOCK_OUTPUT 1 // contain maxlink as output value tasks

#define BUFFER_HEAD_LEN 32
#define BATCH_SIZE 320

typedef struct {
  uint8_t taskType;
  uint16_t taskCount;
  uint32_t totalSize;
} BlockDescriptorBase;

typedef struct {
  BlockDescriptorBase blockDescBase;
} FixedLenBlockDescriptor;

typedef struct {
  BlockDescriptorBase blockDescBase;
  uint32_t *offsets;
} VarLenBlockDescriptor;

typedef struct {
  uint8_t epochNumber;
  uint8_t blockCnt;
  uint16_t totalSize;
  FixedLenBlockDescriptor fixedLenBlockDescs[NUM_FIXED_LEN_BLOCK_INPUT];
  VarLenBlockDescriptor varLenBlockDescs[NUM_VAR_LEN_BLOCK_INPUT];
  uint16_t *offsets;
} CpuToDpuBufferDescriptor;

typedef struct {
  uint8_t taskType;
} Task;

typedef struct {
  Task base;
  uint8_t *ptr; // key
  uint8_t len;
  TupleIdT tid; // value
  uint32_t hashTableId;
} GetOrInsertReq;

inline bool IsVarLenTask(uint8_t taskType)
{
  // FIXME: complete other conditions
  if ( taskType == GET_OR_INSERT_REQ ) {
    return true;
  } else {
    return false;
  }
}

inline uint16_t GetTaskSize(void *task, bool isVarLen)
{
  // FIXME: complete other tasks
  uint8_t taskType = ((Task*)task)->taskType;
  uint16_t ret = sizeof(BlockDescriptorBase);

  switch(taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    // |key + value + hash_id|
    ret += req->len + sizeof(TupleIdT) + sizeof(uint32_t);
    break;
  }
  default:
    break;
  }
  if (isVarLen) {
    ret += sizeof(uint32_t);
  }
  return ret;
}

void CreateCpuToDpuBufferForEachDPU();
#endif
