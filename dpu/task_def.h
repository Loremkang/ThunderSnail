// replace regular expression
// typedef\s((ALIGN8)\sstruct\s(\{(.|\n)*?\}))\s(.+);
// TASK($5, 0, FIXED, sizeof($5), \n $3)

// define task names, request and response
#define SET_DPU_ID_REQ 0
#define CREATE_INDEX_REQ 1
#define GET_OR_INSERT_REQ 2
#define GET_POINTER_REQ 3
#define UPDATE_POINTER_REQ 4
#define GET_MAX_LINK_SIZE_REQ 5
#define FETCH_MAX_LINK_REQ 6
#define MERGE_MAX_LINK_REQ 7
#define NEW_MAX_LINK_REQ 14

#define EMPTY_RESP 128
#define GET_OR_INSERT_RESP 129
#define GET_POINTER_RESP 130
#define UPDATE_POINTER_RESP 131
#define GET_MAX_LINK_SIZE_RESP 132
#define FETCH_MAX_LINK_RESP 133
#define MERGE_MAX_LINK_RESP 134
#define NEW_MAX_LINK_RESP 135

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