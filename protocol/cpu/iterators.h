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

Offset* OffsetsIteratorGetData(void *itr)
{
  OffsetsIterator *iterator = (OffsetsIterator*)itr;
  return &(iterator->offsets[iterator->index]);
}

Iterator* CreateOffsetsIterator(Offset *offsetsPtr, size_t size)
{
  OffsetsIterator *iterator = malloc(sizeof(OffsetsIterator));
  iterator->iterator = (Iterator) {
    .data = iterator,
    .next = OffsetsIteratorNext,
    .hasNext = OffsetsIteratorHasNext,
    .reset = OffsetsIteratorReset,
  };
  iterator->offsets = offsetsPtr;
  iterator->size = size;
  iterator->index = 0;
  return &(iterator->iterator);
}

#endif
