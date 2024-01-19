#ifndef ITERATORS_H
#define ITERATORS_H

#include <stdint.h>

typedef enum { FixedLengthTask = 0, VariableLengthTask = 1 } TaskLengthType;

typedef struct {
  uint8_t* baseAddr;  // base address of the buffer
  TaskLengthType type;
  Offset* offsets;  // offsets for varlen tasks
  size_t taskSize;  // task size for fixed length tasks
  size_t size;
  size_t index;  // current offsets index
} OffsetsIterator;

static inline void OffsetsIteratorNext(OffsetsIterator* iterator) {
  iterator->index++;
}

static inline int OffsetsIteratorHasNext(OffsetsIterator* iterator) {
  return iterator->index < iterator->size;
}

static inline void OffsetsIteratorReset(OffsetsIterator* iterator) {
  iterator->index = 0;
}

static inline Offset OffsetsIteratorGetOffset(OffsetsIterator* iterator) {
  if (iterator->type == FixedLengthTask) {
    return iterator->taskSize * iterator->index;
  } else {
    return iterator->offsets[iterator->index];
  }
}

static inline uint8_t* OffsetsIteratorGetData(OffsetsIterator* iterator) {
  return iterator->baseAddr + OffsetsIteratorGetOffset(iterator);
}

static inline OffsetsIterator OffsetsIteratorInit(TaskLengthType type,
                                                  uint8_t* basePtr,
                                                  Offset* offsetsPtr,
                                                  size_t size,
                                                  size_t taskSize) {
  OffsetsIterator iterator;
  iterator.baseAddr = basePtr;
  iterator.offsets = offsetsPtr;
  iterator.size = size;
  iterator.type = type;
  iterator.taskSize = taskSize;
  ValidValueCheck(type != FixedLengthTask ||
                  (offsetsPtr == NULL && (size == 0 || taskSize != 0)));
  ValidValueCheck(type != VariableLengthTask ||
                  (offsetsPtr != NULL && taskSize == 0));
  iterator.index = 0;
  return iterator;
}

static inline void OffsetsIteratorJumpToKth(OffsetsIterator* iterator,
                                            uint8_t k) {
  iterator->index = k;
}

static inline uint8_t* OffsetsIteratorGetKthData(OffsetsIterator* iterator,
                                                 uint8_t k) {
  OffsetsIteratorJumpToKth(iterator, k);
  return OffsetsIteratorGetData(iterator);
}

#endif
