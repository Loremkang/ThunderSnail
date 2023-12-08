// replace regular expression
// typedef\s((ALIGN8)\sstruct\s(\{(.|\n)*?\}))\s(.+);
// TASK($5, 0, FIXED, sizeof($5), \n $3)
#pragma once
#ifndef TASK_DEF_H
#define TASH_DEF_H

#define FIXED   true
#define UNFIXED false
#include <stdbool.h>
#include "../common_base_struct/common_base_struct.h"

TASK(SetDpuIdReq, 0, FIXED, sizeof(SetDpuIdReq), {
  Task base;
  uint32_t dpuId;
})

TASK(CreateIndexReq, 1, FIXED, sizeof(CreateIndexReq), {
  Task base;
  HashTableId hashTableId;
})

TASK(GetPointerReq, 2, UNFIXED, sizeof(GetPointerReq), {
  Task base;
  uint8_t len;
  HashTableId hashTableId;
  uint8_t ptr[]; // key
})

TASK(GetOrInsertReq, 3, UNFIXED, sizeof(GetOrInsertReq), {
  Task base;
  uint8_t len;  // key len
  TupleIdT tid; // value
  HashTableId hashTableId;
  uint8_t ptr[]; // key
})



TASK(UpdatePointerReq, 4, FIXED, sizeof(UpdatePointerReq), {
  Task base;
  HashAddrT hashEntry;
  MaxLinkAddrT maxLinkAddr;
})

TASK(GetMaxLinkSizeReq, 5, UNFIXED, sizeof(GetMaxLinkSizeReq), {
  Task base;
  MaxLinkAddrT maxLinkAddr;
})

TASK(FetchMaxLinkReq, 6, FIXED, sizeof(FetchMaxLinkReq), {
  Task base;
  MaxLinkAddrT maxLinkAddr;
})

TASK(MergeMaxLinkReq, 7, UNFIXED, sizeof(MergeMaxLinkReq), {
  Task base;
  RemotePtrT ptr;
  MaxLinkT maxLink;
})

TASK(GetOrInsertResp, 8, FIXED, sizeof(GetOrInsertResp), {
  Task base;
  HashTableQueryReplyT tupleIdOrMaxLinkAddr;
})

TASK(GetPointerResp, 9, FIXED, sizeof(GetPointerResp), {
  Task base;
  MaxLinkAddrT maxLinkAddr;
})


TASK(GetMaxLinkSizeResp, 11, FIXED, sizeof(GetMaxLinkSizeResp), {
  Task base;
  uint8_t maxLinkSize;
})


TASK(FetchMaxLinkResp, 12, UNFIXED, sizeof(FetchMaxLinkResp), {
  Task base;
  MaxLinkT maxLink;
})

TASK(NewMaxLinkReq, 14, FIXED, sizeof(NewMaxLinkReq), {
  Task base;
  MaxLinkT maxLink;
})

TASK(NewMaxLinkResp, 15, FIXED, sizeof(NewMaxLinkResp), {
  Task base;
  RemotePtrT ptr;
})
#endif