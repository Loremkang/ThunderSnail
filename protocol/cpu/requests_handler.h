#ifndef REQUESTS_HANDLER_H
#define REQUESTS_HANDLER_H

#include "../protocol.h"

void GetBufferHeader(uint8_t *buffer, DpuBufferHeader *header);

uint8_t GetBufferState(uint8_t *buffer);

uint8_t GetBlockCnt(uint8_t *buffer);

uint32_t GetBufferTotalSize(uint8_t *buffer);

void GetBlockDescriptorBase(uint8_t *blockPtr, BlockDescriptorBase *header);

uint8_t GetTaskType(uint8_t *blockPtr);

uint16_t GetTaskCount(uint8_t *blockPtr);

uint16_t GetBlockTotalSize(uint8_t *blockPtr);

Offset* GetBufferOffsetsPtr(uint8_t *buffer);

#endif
