#include "variable_length_struct_buffer.h"

void ExpendableBufferInit(ExpendableBuffer *buf) {
    buf->capacity = 0;
    buf->size = 0;
    buf->buffer = NULL;
}

void ExpendableBufferFree(ExpendableBuffer *buf) {
    if (buf->buffer != NULL) {
        free(buf->buffer);
    }
    buf->buffer = NULL;
}

static inline bool ExpendableBufferExpand(ExpendableBuffer *buf, OffsetT capacity) {
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

OffsetT ExpendableBufferAppend(ExpendableBuffer *buf, uint8_t* data, OffsetT length) {
    ValidValueCheck(length > 0);
    if (buf->size + length > buf->capacity) {
        ExpendableBufferExpand(buf, (buf->capacity + length) * 2);
    }
    ValidValueCheck(buf->size + length <= buf->capacity);
    memcpy(buf->buffer + buf->size, data, length);
    buf->size += length;
    return buf->size - length;
}

void VariableLengthStructBufferInit(VariableLengthStructBuffer *buf) {
    buf->data = malloc(sizeof(ExpendableBuffer));
    buf->offset = malloc(sizeof(ExpendableBuffer));
    ExpendableBufferInit(buf->offset);
    ExpendableBufferInit(buf->data);
}

OffsetT VariableLengthStructBufferAppend(VariableLengthStructBuffer *buf, uint8_t* data, OffsetT length) {
    ValidValueCheck(length > 0);
    OffsetT offset = ExpendableBufferAppend(buf->data, data, length);
    OffsetT count = ExpendableBufferAppend(buf->offset, (uint8_t*)&offset, sizeof(OffsetT)) / sizeof(OffsetT);
    ValidValueCheck(count == buf->count);
    return buf->count ++;
}

uint8_t* VariableLengthStructBufferGet(VariableLengthStructBuffer *buf, OffsetT idx) {
    ArrayOverflowCheck(idx < buf->count);
    OffsetT* offset = (OffsetT*)buf->offset->buffer;
    ArrayOverflowCheck(offset[idx] < buf->data->size);
    return buf->data->buffer + offset[idx];
}

OffsetT VariableLengthStructBufferGetSize(VariableLengthStructBuffer *buf, OffsetT idx) {
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

void VariableLengthStructBufferFree(VariableLengthStructBuffer *buf) {
    ExpendableBufferFree(buf->offset);
    ExpendableBufferFree(buf->data);
    free(buf->data);
    free(buf->offset);
}

// artificially use uint64_t rather than int
bool VariableLengthStructBufferTest() {
    const int MAXN = 100;
    VariableLengthStructBuffer *buf = malloc(sizeof(VariableLengthStructBuffer));
    VariableLengthStructBufferInit(buf);
    for (int i = 1; i < MAXN; i ++) {
        uint64_t* arr = malloc(i * sizeof(uint64_t));
        for (int j = 0; j < i; j ++) {
            arr[j] = j + 1;
        }
        assert(VariableLengthStructBufferAppend(buf, (uint8_t*)arr, i * sizeof(uint64_t)) == i - 1);
    }
    for (int i = 1; i < MAXN; i ++) {
        uint64_t* arr = (uint64_t*)VariableLengthStructBufferGet(buf, i - 1);
        OffsetT length = VariableLengthStructBufferGetSize(buf, i - 1);
        // printf("i=%d\tlength=%d\n", i, length);
        assert(length == i * sizeof(uint64_t));
        for (int j = 0; j < i; j ++) {
            // printf("arr[%d][%d]=%llu\n", i, j, arr[j]);
            assert(arr[j] == j + 1);
        }
    }
    VariableLengthStructBufferFree(buf);
    free(buf);
    return true;
}

