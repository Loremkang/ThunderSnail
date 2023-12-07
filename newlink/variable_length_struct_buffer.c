#include "variable_length_struct_buffer.h"

void ExpendableBufferInit(ExpendableBufferT *buf) {
    buf->capacity = 0;
    buf->size = 0;
    buf->buffer = NULL;
}

void ExpendableBufferFree(ExpendableBufferT *buf) {
    if (buf->buffer != NULL) {
        free(buf->buffer);
    }
    buf->buffer = NULL;
}

void ExpendableBufferSoftReset(ExpendableBufferT *buf) {
    buf->size = buf->capacity = 0;
}

static inline bool ExpendableBufferExpand(ExpendableBufferT *buf, OffsetT capacity) {
    if (buf->capacity >= capacity) {
        return false;
    }
    // printf("Expand From %d to %d\n", buf->capacity, capacity);
    uint8_t* nextBuffer = malloc(capacity);
    memcpy(nextBuffer, buf->buffer, buf->capacity);
    buf->capacity = capacity;
    free(buf->buffer);
    buf->buffer = nextBuffer;
    return true;
}

OffsetT ExpendableBufferAppendPlaceholder(ExpendableBufferT *buf, OffsetT length) {
    ValidValueCheck(length > 0);
    if (buf->size + length > buf->capacity) {
        ExpendableBufferExpand(buf, (buf->capacity + length) * 2);
    }
    ValidValueCheck(buf->size + length <= buf->capacity);
    buf->size += length;
    return buf->size - length;
}

OffsetT ExpendableBufferAppend(ExpendableBufferT *buf, uint8_t* data, OffsetT length) {
    OffsetT offset = ExpendableBufferAppendPlaceholder(buf, length);
    memcpy(buf->buffer + offset, data, length);
    return offset;
}

void VariableLengthStructBufferInit(VariableLengthStructBufferT *buf) {
    buf->count = 0;
    buf->data = malloc(sizeof(ExpendableBufferT));
    buf->offset = malloc(sizeof(ExpendableBufferT));
    ExpendableBufferInit(buf->offset);
    ExpendableBufferInit(buf->data);
}

void VariableLengthStructBufferSoftReset(VariableLengthStructBufferT *buf) {
    buf->count = 0;
    ExpendableBufferSoftReset(buf->data);
    ExpendableBufferSoftReset(buf->offset);
}

OffsetT VariableLengthStructBufferAppendPlaceholder(VariableLengthStructBufferT *buf, OffsetT length) {
    ValidValueCheck(length > 0);
    OffsetT offset = ExpendableBufferAppendPlaceholder(buf->data, length);
    OffsetT count = ExpendableBufferAppend(buf->offset, (uint8_t*)&offset, sizeof(OffsetT)) / sizeof(OffsetT);
    ValidValueCheck(count == buf->count);
    return buf->count ++;
}

OffsetT VariableLengthStructBufferAppend(VariableLengthStructBufferT *buf, uint8_t* data, OffsetT length) {
    ValidValueCheck(length > 0);
    OffsetT bufferCount = buf->count;
    OffsetT offset = VariableLengthStructBufferAppendPlaceholder(buf, length);
    memcpy(VariableLengthStructBufferGet(buf, offset), data, length);
    return bufferCount;
}

uint8_t* VariableLengthStructBufferGet(VariableLengthStructBufferT *buf, OffsetT idx) {
    ArrayOverflowCheck(idx < buf->count);
    OffsetT* offset = (OffsetT*)buf->offset->buffer;
    ArrayOverflowCheck(offset[idx] < buf->data->size);
    return buf->data->buffer + offset[idx];
}

OffsetT VariableLengthStructBufferGetSize(VariableLengthStructBufferT *buf, OffsetT idx) {
    ArrayOverflowCheck(idx < buf->count);
    if (idx < buf->count - 1) {
        ArrayOverflowCheck((idx + 1) * sizeof(OffsetT) < buf->offset->size);
        OffsetT* offset = (OffsetT*)buf->offset->buffer;
        return offset[idx + 1] - offset[idx];
    } else {
        ValidValueCheck(idx == buf->count - 1);
        OffsetT* offset = (OffsetT*)buf->offset->buffer;
        return buf->data->size - offset[idx];
    }
}

void VariableLengthStructBufferFree(VariableLengthStructBufferT *buf) {
    ExpendableBufferFree(buf->offset);
    ExpendableBufferFree(buf->data);
    free(buf->data);
    buf->data = NULL;
    free(buf->offset);
    buf->offset = NULL;
}