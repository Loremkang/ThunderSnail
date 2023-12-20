#include "newlink.h"
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

void NewLinkPrint(NewLinkT* newLink) {
    printf("NewLink: %p\n", newLink);
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
        printf("(HashAddrT) {.edgeId = %d, .rPtr = ", hashAddr.edgeId);
        RemotePtrPrint(hashAddr.rPtr);
    
    }
}