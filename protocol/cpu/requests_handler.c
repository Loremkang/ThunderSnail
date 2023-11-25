#include "requests_handler.h"

void GetHeader(uint8_t *buffer, DpuBufferHeader *header)
{
  header = (DpuBufferHeader*)buffer;
  return;
}

uint8_t GetBufferState(uint8_t *buffer)
{
  return ((DpuBufferHeader*)buffer)->bufferState;
}

uint8_t GetBlockCnt(uint8_t *buffer)
{
  return ((DpuBufferHeader*)buffer)->blockCnt;
}

uint32_t GetTotalSize(uint8_t *buffer)
{
  return ((DpuBufferHeader*)buffer)->totalSize;
}



