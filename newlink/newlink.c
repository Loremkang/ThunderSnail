#include "newlink.h"
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

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
    printf("NewLink: %" PRIu64 "\n", (uint64_t)newLink);
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