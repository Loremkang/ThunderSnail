#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>
#include "../common_base_struct/common_base_struct.h"

typedef uint32_t HashTableId;
typedef uint32_t Offset; // The buffer offset

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

#define CPU_BUFFER_HEAD_LEN 6 // |epochNumber blockCnt totalSize|
#define DPU_BUFFER_HEAD_LEN 6 // |bufferState blockCnt totalSize|
#define BLOCK_HEAD_LEN sizeof(BlockDescriptorBase)
#define BATCH_SIZE 320
#define NUM_BLOCKS 8

#define BUFFER_LEN 65535

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
  Offset *offsets;
} VarLenBlockDescriptor;

typedef struct {
  uint8_t epochNumber;
  uint8_t blockCnt;
  uint32_t totalSize;
} CpuBufferHeader;

typedef struct {
  CpuBufferHeader header;
  FixedLenBlockDescriptor fixedLenBlockDescs[NUM_FIXED_LEN_BLOCK_INPUT];
  VarLenBlockDescriptor varLenBlockDescs[NUM_VAR_LEN_BLOCK_INPUT];
  Offset *offsets;
} CpuToDpuBufferDescriptor;

typedef struct {
  uint8_t bufferState;
  uint8_t blockCnt;
  uint32_t totalSize;
} DpuBufferHeader;

typedef struct {
  DpuBufferHeader header;
  FixedLenBlockDescriptor fixedLenBlockDescs[NUM_FIXED_LEN_BLOCK_OUTPUT];
  VarLenBlockDescriptor varLenBlockDescs[NUM_VAR_LEN_BLOCK_OUTPUT];
  Offset offsets[NUM_BLOCKS];
} DpuToCpuBufferDescriptor;

typedef struct {
  uint8_t taskType;
} Task;

typedef struct {
  Task base;
  uint8_t len;  // key len
  TupleIdT tid; // value
  HashTableId hashTableId;
  uint8_t ptr[]; // key
} GetOrInsertReq;

typedef struct {
  Task base;
  uint8_t len;
  HashTableId hashTableId;
  uint8_t ptr[]; // key
} GetPointerReq;

typedef struct {
  Task base;
  HashAddrT hashEntry;
  MaxLinkAddrT maxLinkAddr;
} UpdatePointerReq;

typedef struct {
  Task base;
  MaxLinkAddrT maxLinkAddr;
} GetMaxLinkSizeReq;

typedef struct {
  Task base;
  MaxLinkAddrT maxLinkAddr;
} FetchMaxLinkReq;

typedef struct {
  Task base;
  MaxLink maxLink;
} MergeMaxLinkReq;

typedef struct {
  Task base;
  HashTableQueryReplyT tupleIdOrMaxLinkAddr;
} GetOrInsertResp;

typedef struct {
  Task base;
  MaxLinkAddrT maxLinkAddr;
} GetPointerResp;

typedef struct {
  Task base;
  uint8_t maxLinkSize;
} GetMaxLinkSizeResp;

typedef struct {
  Task base;
  MaxLink maxLink;
} FetchMaxLinkResp;

bool IsVarLenTask(uint8_t taskType);

uint16_t GetFixedLenTaskSize(void *task);

#endif
