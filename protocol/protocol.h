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

#define ALIGN8 __attribute__((aligned(8)))
#define ROUND_UP_TO_8(x) (((x)+7) &~7) // to align key len to 8

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

typedef struct {
  uint8_t taskType;
} Task;

typedef ALIGN8 struct {
  Task base;
  uint8_t len;  // key len
  TupleIdT tid; // value
  HashTableId hashTableId;
  uint8_t ptr[]; // key
} GetOrInsertReq;

typedef ALIGN8 struct {
  Task base;
  uint8_t len;
  HashTableId hashTableId;
  uint8_t ptr[]; // key
} GetPointerReq;

typedef ALIGN8 struct {
  Task base;
  HashAddrT hashEntry;
  MaxLinkAddrT maxLinkAddr;
} UpdatePointerReq;

typedef ALIGN8 struct {
  Task base;
  MaxLinkAddrT maxLinkAddr;
} GetMaxLinkSizeReq;

typedef ALIGN8 struct {
  Task base;
  MaxLinkAddrT maxLinkAddr;
} FetchMaxLinkReq;

typedef ALIGN8 struct {
  Task base;
  MaxLinkT maxLink;
} MergeMaxLinkReq;

typedef ALIGN8 struct {
  Task base;
  HashTableQueryReplyT tupleIdOrMaxLinkAddr;
} GetOrInsertResp;

typedef ALIGN8 struct {
  Task base;
  MaxLinkAddrT maxLinkAddr;
} GetPointerResp;

typedef ALIGN8 struct {
  Task base;
  uint8_t maxLinkSize;
} GetMaxLinkSizeResp;

typedef ALIGN8 struct {
  Task base;
  MaxLinkT maxLink;
} FetchMaxLinkResp;

bool IsVarLenTask(uint8_t taskType);

uint16_t GetFixedLenTaskSize(void *task);

#endif
