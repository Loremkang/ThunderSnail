#pragma once
#ifndef VARIABLE_LENGTH_STRUCT_BUFFER_H
#define VARIABLE_LENGTH_STRUCT_BUFFER_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "safety_check_macro.h"

// notice: these buffer may dynamically expand, therefore don't store pointers to them
typedef int OffsetT;

typedef struct ExpendableBufferT {
  OffsetT capacity;
  OffsetT size;
  uint8_t* buffer;
} ExpendableBufferT;

void ExpendableBufferInit(ExpendableBufferT* buf);
void ExpendableBufferFree(ExpendableBufferT* buf);
void ExpendableBufferSoftReset(ExpendableBufferT* buf);
int ExpendableBufferAppend(ExpendableBufferT* buf, uint8_t* data,
                           OffsetT length);
OffsetT ExpendableBufferAppendPlaceholder(ExpendableBufferT* buf,
                                          OffsetT length);

typedef struct VariableLengthStructBufferT {
  // number of variable length structs in the buffer
  OffsetT count;
  ExpendableBufferT *offset, *data;
} VariableLengthStructBufferT;

// always call after allocate
void VariableLengthStructBufferInit(VariableLengthStructBufferT* buf);

// reset without releasing memory
void VariableLengthStructBufferSoftReset(VariableLengthStructBufferT* buf);

int VariableLengthStructBufferAppend(VariableLengthStructBufferT* buf,
                                     uint8_t* data, OffsetT length);

// reserve space in the buffer. Return idx. Get address by "VariableLengthStructBufferGet(idx)".
OffsetT VariableLengthStructBufferAppendPlaceholder(
    VariableLengthStructBufferT* buf, OffsetT length);

uint8_t* VariableLengthStructBufferGet(VariableLengthStructBufferT* buf,
                                       OffsetT idx);
OffsetT VariableLengthStructBufferGetSize(VariableLengthStructBufferT* buf,
                                          OffsetT idx);
void VariableLengthStructBufferFree(VariableLengthStructBufferT* buf);

#endif