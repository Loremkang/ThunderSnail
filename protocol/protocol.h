#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include "../common_base_struct/common_base_struct.h"

typedef struct {
  uint8_t taskType;
  uint16_t taskCount;
  uint32_t totalSize;
  void* tasks;
  uint32_t offsets[];
} BlockDescriptor;

typedef struct {
  uint16_t epochNumber;
  uint16_t blockCnt;
  uint16_t totalSize;
  BlockDescriptor* blockDescriptors;
  uint16_t offsets[];
} CpuToDpuBufferDesciptor;

typedef struct {
  uint16_t count; // num of key and value
  struct {
    uint16_t sz; // key's length
    uint16_t pos; // offset of the Request buffer, represent the key's start ptr
    TupleIdT tid;
  } kv;
  uint8_t buffer[]; // The tail buffer to save the key's value
} BatchGetOrInsertReq;

#endif
