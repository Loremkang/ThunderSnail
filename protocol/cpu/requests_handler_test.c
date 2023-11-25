#include "requests_handler.h"
#include "iterators.h"

void TraverseReceiveBuffer(uint8_t *buffer)
{
  Offset *offsetsPtr = GetBufferOffsetsPtr(buffer);
  Iterator *blockIterator = CreateOffsetsIterator(offsetsPtr, GetBlockCnt(buffer));
  while (blockIterator->hasNext(blockIterator->data)) {
    Offset* offset = OffsetsIteratorGetData(blockIterator->data);
    // TODO: process current block

    blockIterator->next(blockIterator->data);
  }
  blockIterator->reset(blockIterator->data);
}
