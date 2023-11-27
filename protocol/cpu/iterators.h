#ifndef ITERATORS_H
#define ITERATORS_H

#include <stdint.h>

typedef struct {
  void *data;
  void (*next) (void *itr);
  int (*hasNext) (void *itr);
  void (*reset) (void *itr);
} Iterator;

typedef struct {
  Iterator iterator;
  uint8_t* baseAddr; // base address of the buffer
  Offset *offsets; // offsets
  size_t size;
  size_t index; // current offsets index
} OffsetsIterator;

void OffsetsIteratorNext(void *itr)
{
  OffsetsIterator *iterator = (OffsetsIterator*)itr;
  iterator->index++;
}

int OffsetsIteratorHasNext(void *itr)
{
  OffsetsIterator *iterator = (OffsetsIterator*)itr;
  return iterator->index < iterator->size;
}

void OffsetsIteratorReset(void *itr)
{
  OffsetsIterator *iterator = (OffsetsIterator*)itr;
  iterator->index = 0;
}

Offset* OffsetsIteratorGetOffset(void *itr)
{
  OffsetsIterator *iterator = (OffsetsIterator*)itr;
  return &(iterator->offsets[iterator->index]);
}

uint8_t* OffsetsIteratorGetData(void *itr)
{
  OffsetsIterator *iterator = (OffsetsIterator*)itr;
  return iterator->baseAddr + iterator->offsets[iterator->index];
}

Iterator* OffsetsIteratorInit(OffsetsIterator* iterator, uint8_t* basePtr, Offset *offsetsPtr, size_t size)
{
  iterator->iterator = (Iterator) {
    .data = iterator,
    .next = OffsetsIteratorNext,
    .hasNext = OffsetsIteratorHasNext,
    .reset = OffsetsIteratorReset,
  };
  iterator->baseAddr = basePtr;
  iterator->offsets = offsetsPtr;
  iterator->size = size;
  iterator->index = 0;
  return &(iterator->iterator);
}

void OffsetIteratorJumpToKth(OffsetsIterator *iterator, uint8_t k)
{
  iterator->index = k;
}

uint8_t* OffsetIteratorGetKthData(OffsetsIterator *iterator, uint8_t k)
{
  OffsetsIteratorJumpToKth(iterator, k);
  OffsetsIteratorGetData(iterator);
}

#endif
