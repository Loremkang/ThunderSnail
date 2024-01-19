#pragma once
#ifndef NEWLINK_TO_MAXLINK_H
#define NEWLINK_TO_MAXLINK_H

#include <string.h>
#include "common_base_struct/common_base_struct.h"
#include "newlink.h"
#include "shared_constants.h"

typedef NewLinkT PreMaxLinkT;

typedef struct NewLinkMergerT {
  int tupleIDCount;
  int maxLinkAddrCount;
  int hashAddrCount;
  TupleIdT tupleIds[MAX_ELEMENT_COUNT_PER_MAXLINK];
  MaxLinkAddrT maxLinkAddrs[MAX_ELEMENT_COUNT_PER_MAXLINK];
  HashAddrT hashAddrs[MAX_ELEMENT_COUNT_PER_MAXLINK];
} NewLinkMergerT;

static inline void NewLinkMergerReset(NewLinkMergerT* merger) {
  merger->tupleIDCount = 0;
  merger->maxLinkAddrCount = 0;
  merger->hashAddrCount = 0;
}

static inline void NewLinkMergeMaxLink(NewLinkMergerT* merger,
                                       MaxLinkT* maxLink) {
  for (int i = 0; i < maxLink->tupleIDCount; i++) {
    bool duplicate = false;
    TupleIdT tupleId = MaxLinkGetTupleIDs(maxLink)[i];
    for (int j = 0; j < merger->tupleIDCount; j++) {
      if (TupleIdEqual(merger->tupleIds[j], tupleId)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      merger->tupleIds[merger->tupleIDCount++] = tupleId;
    }
    ArrayOverflowCheck(merger->tupleIDCount <= MAX_ELEMENT_COUNT_PER_MAXLINK);
  }
  for (int i = 0; i < maxLink->hashAddrCount; i++) {
    bool duplicate = false;
    HashAddrT hashAddr = MaxLinkGetHashAddrs(maxLink)[i];
    for (int j = 0; j < merger->hashAddrCount; j++) {
      if (HashAddrEqual(merger->hashAddrs[j], hashAddr)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      merger->hashAddrs[merger->hashAddrCount++] = hashAddr;
    }
    ArrayOverflowCheck(merger->hashAddrCount <= MAX_ELEMENT_COUNT_PER_MAXLINK);
  }
}

static inline void NewLinkMergeNewLink(NewLinkMergerT* merger,
                                       NewLinkT* newLink) {
  for (int i = 0; i < newLink->tupleIDCount; i++) {
    bool duplicate = false;
    TupleIdT tupleId = NewLinkGetTupleIDs(newLink)[i];
    for (int j = 0; j < merger->tupleIDCount; j++) {
      if (TupleIdEqual(merger->tupleIds[j], tupleId)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      merger->tupleIds[merger->tupleIDCount++] = tupleId;
    }
    ArrayOverflowCheck(merger->tupleIDCount <= MAX_ELEMENT_COUNT_PER_MAXLINK);
  }
  for (int i = 0; i < newLink->maxLinkAddrCount; i++) {
    bool duplicate = false;
    MaxLinkAddrT maxLinkAddr = NewLinkGetMaxLinkAddrs(newLink)[i];
    for (int j = 0; j < merger->maxLinkAddrCount; j++) {
      if (MaxLinkAddrEqual(merger->maxLinkAddrs[j], maxLinkAddr)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      merger->maxLinkAddrs[merger->maxLinkAddrCount++] = maxLinkAddr;
    }
    ArrayOverflowCheck(merger->maxLinkAddrCount <=
                       MAX_ELEMENT_COUNT_PER_MAXLINK);
  }
  for (int i = 0; i < newLink->hashAddrCount; i++) {
    bool duplicate = false;
    HashAddrT hashAddr = NewLinkGetHashAddrs(newLink)[i];
    for (int j = 0; j < merger->hashAddrCount; j++) {
      if (HashAddrEqual(merger->hashAddrs[j], hashAddr)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) {
      merger->hashAddrs[merger->hashAddrCount++] = hashAddr;
    }
    ArrayOverflowCheck(merger->hashAddrCount <= MAX_ELEMENT_COUNT_PER_MAXLINK);
  }
}

static inline size_t NewLinkMergerGetExportSize(NewLinkMergerT* merger) {
  return sizeof(NewLinkT) + sizeof(TupleIdT) * merger->tupleIDCount +
         sizeof(MaxLinkAddrT) * merger->maxLinkAddrCount +
         sizeof(HashAddrT) * merger->hashAddrCount;
}

static inline void NewLinkMergerExport(NewLinkMergerT* merger,
                                       NewLinkT* target) {
  target->tupleIDCount = merger->tupleIDCount;
  target->maxLinkAddrCount = merger->maxLinkAddrCount;
  target->hashAddrCount = merger->hashAddrCount;
  memcpy(NewLinkGetTupleIDs(target), merger->tupleIds,
         sizeof(TupleIdT) * merger->tupleIDCount);
  memcpy(NewLinkGetMaxLinkAddrs(target), merger->maxLinkAddrs,
         sizeof(MaxLinkAddrT) * merger->maxLinkAddrCount);
  memcpy(NewLinkGetHashAddrs(target), merger->hashAddrs,
         sizeof(HashAddrT) * merger->hashAddrCount);
}

static inline void NewLinkToMaxLink(NewLinkT* newLink, MaxLinkT* maxLink) {
  maxLink->tupleIDCount = newLink->tupleIDCount;
  maxLink->hashAddrCount = newLink->hashAddrCount;
  memcpy(MaxLinkGetTupleIDs(maxLink), NewLinkGetTupleIDs(newLink),
         sizeof(TupleIdT) * newLink->tupleIDCount);
  memcpy(MaxLinkGetHashAddrs(maxLink), NewLinkGetHashAddrs(newLink),
         sizeof(HashAddrT) * newLink->hashAddrCount);
}

#endif
