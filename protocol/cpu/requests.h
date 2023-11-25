#ifndef REQUESTS_H
#define REQUESTS_H

#include "../protocol.h"
#include "cpu_buffer_builder.h"

#define NUM_DPU 64

typedef struct {
  uint8_t *data;
  uint32_t len;
} Key;

static uint8_t epochNumber = 0;

inline static uint8_t GetEpochNumber()
{
  if (epochNumber == 255) {
    epochNumber = 0;
    return 255;
  }
  return epochNumber++;
}

void SendGetOrInsertReq(uint32_t tableId, Key *keys, uint64_t *tupleAddr, size_t batchSize, uint8_t *recvBuffers[]);

#endif
