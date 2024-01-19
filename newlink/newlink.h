#pragma once
#ifndef NEWLINK_H
#define NEWLINK_H

#include "common_base_struct/common_base_struct.h"

typedef struct {
  int tupleIDCount;
  int maxLinkAddrCount;
  int hashAddrCount;
  int padding;
  uint8_t buffer[];  // [[Tuple IDs], [MaxLink Addrs], [Hash Addrs]]
} NewLinkT;

static inline TupleIdT* NewLinkGetTupleIDs(NewLinkT* newLink) {
  return (TupleIdT*)(newLink->buffer);
}

static inline MaxLinkAddrT* NewLinkGetMaxLinkAddrs(NewLinkT* newLink) {
  return (MaxLinkAddrT*)(newLink->buffer +
                         sizeof(TupleIdT) * newLink->tupleIDCount);
}

static inline HashAddrT* NewLinkGetHashAddrs(NewLinkT* newLink) {
  return (HashAddrT*)(newLink->buffer +
                      sizeof(TupleIdT) * newLink->tupleIDCount +
                      sizeof(MaxLinkAddrT) * newLink->maxLinkAddrCount);
}

static inline int NewLinkGetSize(int tupleIdCount, int maxLinkAddrCount,
                                 int hashAddrCount) {
  return sizeof(NewLinkT) + sizeof(TupleIdT) * tupleIdCount +
         sizeof(MaxLinkAddrT) * maxLinkAddrCount +
         sizeof(HashAddrT) * hashAddrCount;
}

void NewLinkPrint(NewLinkT* newLink);

#endif