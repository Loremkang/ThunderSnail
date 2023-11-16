
#include "newlink.h"
#include <stdio.h>
#include <stdlib.h>

TupleIdT* NewLinkGetTupleIDs(NewLinkT* newLink) {
    return (TupleIdT*)(newLink->buffer);
}

MaxLinkAddrT* NewLinkGetMaxLinkAddrs(NewLinkT* newLink) {
    return (MaxLinkAddrT*)(newLink->buffer + sizeof(TupleIdT) * newLink->tupleIDCount);
}

int NewLinkGetSize(int tupleIdCount, int maxLinkAddrCount) {
    return sizeof(NewLinkT) + sizeof(TupleIdT) * tupleIdCount + sizeof(MaxLinkAddrT) * maxLinkAddrCount;
}

void NewLinkPrint(NewLinkT* newLink) {
    printf("NewLink: %llx\n", (uint64_t)newLink);
    printf("TupleIdCount = %d\n", newLink->tupleIDCount);
    printf("MaxLinkAddrCount = %d\n", newLink->maxLinkAddrCount);
    for (int i = 0; i < newLink->tupleIDCount; i ++) {
        TupleIdT tupleId = NewLinkGetTupleIDs(newLink)[i];
        TupleIdPrint(tupleId);
    }
    for (int i = 0; i < newLink->maxLinkAddrCount; i ++) {
        MaxLinkAddrT maxLinkAddr = NewLinkGetMaxLinkAddrs(newLink)[i];
        printf("MaxLinkAddrT.rPtr = ");
        RemotePtrPrint(maxLinkAddr.rPtr);
    }
}

void NewLinkTest() {
    NewLinkT* newLink = (NewLinkT*)malloc(NewLinkGetSize(3, 3));
    printf("size=%d\n", NewLinkGetSize(3, 3));
    newLink->maxLinkAddrCount = newLink->tupleIDCount = 3;
    NewLinkGetTupleIDs(newLink)[0] = (TupleIdT){.tableId = 1, .tupleAddr = 0};
    NewLinkGetTupleIDs(newLink)[1] = (TupleIdT){.tableId = 1, .tupleAddr = 1};
    NewLinkGetTupleIDs(newLink)[2] = (TupleIdT){.tableId = 1, .tupleAddr = 2};
    NewLinkGetMaxLinkAddrs(newLink)[0] = (MaxLinkAddrT){.rPtr = (RemotePtrT){.id = 2, .addr = 0}};
    NewLinkGetMaxLinkAddrs(newLink)[1] = (MaxLinkAddrT){.rPtr = (RemotePtrT){.id = 2, .addr = 1}};
    NewLinkGetMaxLinkAddrs(newLink)[2] = (MaxLinkAddrT){.rPtr = (RemotePtrT){.id = 2, .addr = 2}};
    NewLinkPrint(newLink);
    free(newLink);
}