// define task names, request and response
#ifndef TASK_DEF_H
#define TASK_DEF_H

#define SET_DPU_ID_REQ 0
#define CREATE_INDEX_REQ 1
#define GET_OR_INSERT_REQ 2
#define GET_POINTER_REQ 3
#define UPDATE_POINTER_REQ 4
#define GET_MAX_LINK_SIZE_REQ 5
#define FETCH_MAX_LINK_REQ 6
#define MERGE_MAX_LINK_REQ 7
#define NEW_MAX_LINK_REQ 8

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

#endif

#ifdef TASK
TASK(SetDpuIdReq, SET_DPU_ID_REQ, FIXED, sizeof(SetDpuIdReq), {
  Task base;
  uint32_t dpuId;
})

TASK(CreateIndexReq, CREATE_INDEX_REQ, FIXED, sizeof(CreateIndexReq), {
  Task base;
  HashTableId hashTableId;
})

TASK(GetOrInsertReq, GET_OR_INSERT_REQ, UNFIXED, sizeof(GetOrInsertReq), {
  Task base;
  uint8_t len;  // key len
  uint32_t taskIdx;
  TupleIdT tid; // value
  HashTableId hashTableId;
  uint8_t ptr[]; // key
})

TASK(GetPointerReq, GET_POINTER_REQ, UNFIXED, sizeof(GetPointerReq), {
  Task base;
  uint8_t len;
  HashTableId hashTableId;
  uint8_t ptr[]; // key
})

TASK(UpdatePointerReq, UPDATE_POINTER_REQ, FIXED, sizeof(UpdatePointerReq), {
  Task base;
  HashAddrT hashEntry;
  MaxLinkAddrT maxLinkAddr;
})

TASK(GetMaxLinkSizeReq, GET_MAX_LINK_SIZE_REQ, FIXED, sizeof(GetMaxLinkSizeReq), {
  Task base;
  MaxLinkAddrT maxLinkAddr;
})

TASK(FetchMaxLinkReq, FETCH_MAX_LINK_REQ, FIXED, sizeof(FetchMaxLinkReq), {
  Task base;
  MaxLinkAddrT maxLinkAddr;
})

TASK(MergeMaxLinkReq, MERGE_MAX_LINK_REQ, UNFIXED, sizeof(MergeMaxLinkReq), {
  Task base;
  RemotePtrT ptr;
  MaxLinkT maxLink;
})

TASK(NewMaxLinkReq, NEW_MAX_LINK_REQ, UNFIXED, sizeof(NewMaxLinkReq), {
  Task base;
  MaxLinkT maxLink;
})


TASK(GetOrInsertResp, GET_OR_INSERT_RESP, FIXED, sizeof(GetOrInsertResp), {
  Task base;
  uint32_t taskIdx;
  HashTableQueryReplyT tupleIdOrMaxLinkAddr;
})

TASK(GetPointerResp, GET_POINTER_RESP, FIXED, sizeof(GetPointerResp), {
  Task base;
  MaxLinkAddrT maxLinkAddr;
})


TASK(GetMaxLinkSizeResp, GET_MAX_LINK_SIZE_RESP, FIXED, sizeof(GetMaxLinkSizeResp), {
  Task base;
  uint8_t maxLinkSize;
})


TASK(FetchMaxLinkResp, FETCH_MAX_LINK_RESP, UNFIXED, sizeof(FetchMaxLinkResp), {
  Task base;
  MaxLinkT maxLink;
})

TASK(NewMaxLinkResp, NEW_MAX_LINK_RESP, FIXED, sizeof(NewMaxLinkResp), {
  Task base;
  RemotePtrT ptr;
})
#undef TASK
#endif