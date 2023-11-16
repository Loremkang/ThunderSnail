#pragma once

#ifndef VARIABLE_LENGTH_STRUCT_BUFFER_H
#define VARIABLE_LENGTH_STRUCT_BUFFER_H

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "../safety_check_macro.h"

typedef int OffsetT;
typedef struct ExpendableBufferT {
    OffsetT capacity;
    OffsetT size;
    uint8_t *buffer;
} ExpendableBufferT;

void ExpendableBufferInit(ExpendableBufferT *buf);
void ExpendableBufferFree(ExpendableBufferT *buf);
int ExpendableBufferAppend(ExpendableBufferT *buf, uint8_t* data, OffsetT length);

typedef struct VariableLengthStructBufferT {
    OffsetT count;
    ExpendableBufferT *offset, *data;
} VariableLengthStructBufferT;

void VariableLengthStructBufferInit(VariableLengthStructBufferT *buf);
int VariableLengthStructBufferAppend(VariableLengthStructBufferT *buf, uint8_t* data, OffsetT length);
uint8_t* VariableLengthStructBufferGet(VariableLengthStructBufferT *buf, OffsetT idx);
OffsetT VariableLengthStructBufferGetSize(VariableLengthStructBufferT *buf, OffsetT idx);
void VariableLengthStructBufferFree(VariableLengthStructBufferT *buf);
bool VariableLengthStructBufferTest();

#endif