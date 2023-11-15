#pragma once

#ifndef VARIABLE_LENGTH_STRUCT_BUFFER_H
#define VARIABLE_LENGTH_STRUCT_BUFFER_H

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "../safety_check_macro.h"

typedef int OffsetT;
typedef struct ExpendableBuffer {
    OffsetT capacity;
    OffsetT size;
    uint8_t *buffer;
} ExpendableBuffer;

void ExpendableBufferInit(ExpendableBuffer *buf);
void ExpendableBufferFree(ExpendableBuffer *buf);
int ExpendableBufferAppend(ExpendableBuffer *buf, uint8_t* data, OffsetT length);

typedef struct VariableLengthStructBuffer {
    OffsetT count;
    ExpendableBuffer *offset, *data;
} VariableLengthStructBuffer;

void VariableLengthStructBufferInit(VariableLengthStructBuffer *buf);
int VariableLengthStructBufferAppend(VariableLengthStructBuffer *buf, uint8_t* data, OffsetT length);
uint8_t* VariableLengthStructBufferGet(VariableLengthStructBuffer *buf, OffsetT idx);
OffsetT VariableLengthStructBufferGetSize(VariableLengthStructBuffer *buf, OffsetT idx);
void VariableLengthStructBufferFree(VariableLengthStructBuffer *buf);
bool VariableLengthStructBufferTest();

#endif