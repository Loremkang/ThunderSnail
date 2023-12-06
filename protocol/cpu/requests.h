#ifndef REQUESTS_H
#define REQUESTS_H

#include "../protocol.h"
#include "dpu.h"

#ifndef DPU_BINARY
#define DPU_BINARY "dpu_task"
#endif

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
void SendSetDpuIdReq(struct dpu_set_t set);
void SendCreateIndexReq(struct dpu_set_t set, HashTableId indexId);
void SendGetOrInsertReq(struct dpu_set_t set, uint32_t tableId, HashTableId hashTableId, Key *keys, uint64_t *tupleAddrs,
                        size_t batchSize, uint8_t *recvBuffers[]);
#endif
