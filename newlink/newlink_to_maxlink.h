#pragma once
#ifndef NEWLINK_TO_MAXLINK_H
#define NEWLINK_TO_MAXLINK_H

#include "newlink.h"
#include "shared_constants.h"
#include "../common_base_struct/common_base_struct.h"
#include <string.h>

typedef NewLinkT PreMaxLinkT;

typedef struct NewLinkMergerT {
    int tupleIDCount;
    int maxLinkAddrCount;
    int hashAddrCount;
    TupleIdT tupleIds[MAXSIZE_MAXLINK];
    MaxLinkAddrT maxLinkAddrs[MAXSIZE_MAXLINK];
    HashAddrT hashAddrs[MAXSIZE_MAXLINK];
} NewLinkMergerT;

inline void NewLinkMergerInit(NewLinkMergerT *merger) {
    merger->tupleIDCount = 0;
    merger->maxLinkAddrCount = 0;
    merger->hashAddrCount = 0;
}

static inline void NewLinkMerge(NewLinkMergerT *merger, NewLinkT *newLink) {
    for (int i = 0; i < newLink->tupleIDCount; i ++) {
        bool duplicate = false;
        TupleIdT tupleId = NewLinkGetTupleIDs(newLink)[i];
        for (int j = 0; j < merger->tupleIDCount; j ++) {
            if (TupleIdEqual(merger->tupleIds[j], tupleId)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            merger->tupleIds[merger->tupleIDCount ++] = tupleId;
        }
        ArrayOverflowCheck(merger->tupleIDCount <= MAXSIZE_MAXLINK);
    }
    for (int i = 0; i < newLink->maxLinkAddrCount; i ++) {
        bool duplicate = false;
        MaxLinkAddrT maxLinkAddr = NewLinkGetMaxLinkAddrs(newLink)[i];
        for (int j = 0; j < merger->maxLinkAddrCount; j ++) {
            if (MaxLinkAddrEqual(merger->maxLinkAddrs[j], maxLinkAddr)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            merger->maxLinkAddrs[merger->maxLinkAddrCount ++] = maxLinkAddr;
        }
        ArrayOverflowCheck(merger->maxLinkAddrCount <= MAXSIZE_MAXLINK);
    }
    for (int i = 0; i < newLink->hashAddrCount; i ++) {
        bool duplicate = false;
        HashAddrT hashAddr = NewLinkGetHashAddrs(newLink)[i];
        for (int j = 0; j < merger->hashAddrCount; j ++) {
            if (HashAddrEqual(merger->hashAddrs[j], hashAddr)) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            merger->hashAddrs[merger->hashAddrCount ++] = hashAddr;
        }
        ArrayOverflowCheck(merger->hashAddrCount <= MAXSIZE_MAXLINK);
    }
}

static inline void NewLinkMergerExport(NewLinkMergerT *merger, NewLinkT *target) {
    target->tupleIDCount = merger->tupleIDCount;
    target->maxLinkAddrCount = merger->maxLinkAddrCount;
    target->hashAddrCount = merger->hashAddrCount;
    memcpy(NewLinkGetTupleIDs(target), merger->tupleIds, sizeof(TupleIdT) * merger->tupleIDCount);
    memcpy(NewLinkGetMaxLinkAddrs(target), merger->maxLinkAddrs, sizeof(MaxLinkAddrT) * merger->maxLinkAddrCount);
    memcpy(NewLinkGetHashAddrs(target), merger->hashAddrs, sizeof(HashAddrT) * merger->hashAddrCount);
}

static inline void NewLinkToMaxLink(NewLinkT *newLink, MaxLinkT *maxLink) {
    maxLink->tupleIDCount = newLink->tupleIDCount;
    maxLink->hashAddrCount = newLink->hashAddrCount;
    memcpy(GetTupleIDsFromMaxLink(maxLink), NewLinkGetTupleIDs(newLink), sizeof(TupleIdT) * newLink->tupleIDCount);
    memcpy(GetHashAddrsFromMaxLink(maxLink), NewLinkGetHashAddrs(newLink), sizeof(HashAddrT) * newLink->hashAddrCount);
}

#endif
