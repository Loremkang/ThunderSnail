#ifndef REQUESTS_HANDLER_H
#define REQUESTS_HANDLER_H

#include "../protocol.h"

void GetHeader(uint8_t *buffer, DpuBufferHeader *header);

uint8_t GetBufferState(uint8_t *buffer);

uint8_t GetBlockCnt(uint8_t *buffer);

uint32_t GetTotalSize(uint8_t *buffer);

#endif
