#include "newlink_to_maxlink.h"
#include "shared_constants.h"
#include "variable_length_struct_buffer.h"
#include "common_base_struct.h"

void GenerateGetSizeTask(MaxLinkAddrT addr) {
    Unimplemented("GenerateGetSizeTask");
}

int FetchGetSizeReply(int idx) {
    Unimplemented("FetchGetSizeReply");
    return 0;
}

void GenerateFetchMaxLinkTask(MaxLinkAddrT addr) {
    Unimplemented("GenerateFetchMaxLinkTask");
}

MaxLinkT* FetchFetchMaxLinkReply(int idx) {
    Unimplemented("FetchGetSizeReply");
    return NULL;
}

void BuildPreMaxLinkFromNewLink(VariableLengthStructBufferT* newLinkBuffer, VariableLengthStructBufferT* preMaxLinkBuffer) {
    static int largestMaxLinkPos[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    static int largestMaxLinkSize[MAXSIZE_HASH_TABLE_QUERY_BATCH];
    static NewLinkMergerT merger;
    ArrayOverflowCheck(newLinkBuffer->count <= MAXSIZE_HASH_TABLE_QUERY_BATCH);

    // int taskCount = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i ++) {
        NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
            GenerateGetSizeTask(NewLinkGetMaxLinkAddrs(newLink)[j]);
            // taskCount ++;
        }
    }

    // run tasks : 1 IO Round

    int taskCount = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i ++) {
        largestMaxLinkPos[i] = -1;
        largestMaxLinkSize[i] = 0;
        NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
            int size = FetchGetSizeReply(taskCount++);
            if (largestMaxLinkPos[i] == -1 || size > largestMaxLinkSize[i]) {
                largestMaxLinkPos[i] = j;
                largestMaxLinkSize[i] = size;
            }
        }
    }

    for (OffsetT i = 0; i < newLinkBuffer->count; i ++) {
        NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
            if (j != largestMaxLinkPos[i]) {
                GenerateFetchMaxLinkTask(NewLinkGetMaxLinkAddrs(newLink)[j]);
            }
        }
    }


    // run tasks : 1 IO Round

    Unimplemented("Fix Error");
    // NewLinkMergerInit(&merger);
    
    taskCount = 0;
    for (OffsetT i = 0; i < newLinkBuffer->count; i ++) {
        NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkBuffer, i);
        for (int j = 0; j < newLink->maxLinkAddrCount; j ++) {
            if (j != largestMaxLinkPos[i]) {
                MaxLinkT* maxLink = FetchFetchMaxLinkReply(taskCount++);
                ValidValueCheck(newLink->maxLinkAddrCount == 0);
                Unimplemented("Fix Error");
                // NewLinkMerge(&merger, newLink);
            } else {
                merger.maxLinkAddrs[merger.maxLinkAddrCount ++] = NewLinkGetMaxLinkAddrs(newLink)[j];
            }
        }
        ValidValueCheck(merger.maxLinkAddrCount <= 1);
        int size = NewLinkGetSize(merger.tupleIDCount, merger.maxLinkAddrCount, merger.hashAddrCount);
        int idx = VariableLengthStructBufferAppendPlaceholder(preMaxLinkBuffer, size);
        PreMaxLinkT* preMaxLink = (PreMaxLinkT*)VariableLengthStructBufferGet(preMaxLinkBuffer, idx);
        Unimplemented("Fix Error");
        // NewLinkMergerExport(&merger, preMaxLink);
    }

    // reset buffer

    for (OffsetT i = 0; i < preMaxLinkBuffer->count; i ++) {
        PreMaxLinkT* preMaxLink = (PreMaxLinkT*)VariableLengthStructBufferGet(preMaxLinkBuffer, i);
        if (preMaxLink->maxLinkAddrCount == 0) {
            Unimplemented("Send MaxLink to DPU for Creating");
        } else {
            ValidValueCheck(preMaxLink->maxLinkAddrCount == 1);
            Unimplemented("Send MaxLink to DPU for Merging");
        }
    }
}