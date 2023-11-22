
#include "newlink.h"
#include <stdio.h>
#include <stdlib.h>

TupleIdT* NewLinkGetTupleIDs(NewLinkT* newLink) {
    return (TupleIdT*)(newLink->buffer);
}

MaxLinkAddrT* NewLinkGetMaxLinkAddrs(NewLinkT* newLink) {
    return (MaxLinkAddrT*)(newLink->buffer +
                           sizeof(TupleIdT) * newLink->tupleIDCount);
}

HashAddrT* NewLinkGetHashAddrs(NewLinkT* newLink) {
    return (HashAddrT*)(newLink->buffer +
                        sizeof(TupleIdT) * newLink->tupleIDCount +
                        sizeof(MaxLinkAddrT) * newLink->maxLinkAddrCount);
}

int NewLinkGetSize(int tupleIdCount, int maxLinkAddrCount, int hashAddrCount) {
    return sizeof(NewLinkT) + sizeof(TupleIdT) * tupleIdCount +
           sizeof(MaxLinkAddrT) * maxLinkAddrCount +
           sizeof(HashAddrT) * hashAddrCount;
}

void NewLinkPrint(NewLinkT* newLink) {
    printf("NewLink: %llx\n", (uint64_t)newLink);
    printf("TupleIdCount = %d\n", newLink->tupleIDCount);
    printf("MaxLinkAddrCount = %d\n", newLink->maxLinkAddrCount);
    printf("HashAddrCount = %d\n", newLink->hashAddrCount);
    for (int i = 0; i < newLink->tupleIDCount; i++) {
        TupleIdT tupleId = NewLinkGetTupleIDs(newLink)[i];
        TupleIdPrint(tupleId);
    }
    for (int i = 0; i < newLink->maxLinkAddrCount; i++) {
        MaxLinkAddrT maxLinkAddr = NewLinkGetMaxLinkAddrs(newLink)[i];
        printf("MaxLinkAddrT.rPtr = ");
        RemotePtrPrint(maxLinkAddr.rPtr);
    }
    for (int i = 0; i < newLink->hashAddrCount; i ++) {
        HashAddrT hashAddr = NewLinkGetHashAddrs(newLink)[i];
        printf("HashAddrT.rPtr = ");
        RemotePtrPrint(hashAddr.rPtr);
    
    }
}

void NewLinkTest() {
    NewLinkT* newLink = (NewLinkT*)malloc(NewLinkGetSize(3, 3, 3));
    printf("size=%d\n", NewLinkGetSize(3, 3, 3));
    newLink->maxLinkAddrCount = newLink->tupleIDCount = 3;
    NewLinkGetTupleIDs(newLink)[0] = (TupleIdT){.tableId = 0, .tupleAddr = 0};
    NewLinkGetTupleIDs(newLink)[1] = (TupleIdT){.tableId = 0, .tupleAddr = 1};
    NewLinkGetTupleIDs(newLink)[2] = (TupleIdT){.tableId = 0, .tupleAddr = 2};
    NewLinkGetMaxLinkAddrs(newLink)[0] =
        (MaxLinkAddrT){.rPtr = (RemotePtrT){.dpuId = 1, .dpuAddr = 0}};
    NewLinkGetMaxLinkAddrs(newLink)[1] =
        (MaxLinkAddrT){.rPtr = (RemotePtrT){.dpuId = 1, .dpuAddr = 1}};
    NewLinkGetMaxLinkAddrs(newLink)[2] =
        (MaxLinkAddrT){.rPtr = (RemotePtrT){.dpuId = 1, .dpuAddr = 2}};
    NewLinkGetHashAddrs(newLink)[0] =
        (HashAddrT){.rPtr = (RemotePtrT){.dpuId = 2, .dpuAddr = 0}};
    NewLinkGetHashAddrs(newLink)[1] =
        (HashAddrT){.rPtr = (RemotePtrT){.dpuId = 2, .dpuAddr = 1}};
    NewLinkGetHashAddrs(newLink)[2] =
        (HashAddrT){.rPtr = (RemotePtrT){.dpuId = 2, .dpuAddr = 2}};
    NewLinkPrint(newLink);
    free(newLink);
}