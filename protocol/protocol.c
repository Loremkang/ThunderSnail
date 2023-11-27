#include "protocol.h"

bool IsVarLenTask(uint8_t taskType)
{
  if ( taskType == GET_OR_INSERT_REQ ||
       taskType == GET_POINTER_REQ ||
       taskType == MERGE_MAX_LINK_REQ) {
    return true;
  } else {
    return false;
  }
}

uint16_t GetFixedLenTaskSize(void *task)
{
  // FIXME: complete other tasks
  uint8_t taskType = ((Task*)task)->taskType;
  uint16_t ret = 0;

  switch(taskType) {
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    // |key + value + hash_id|
    ret += req->len + sizeof(GetOrInsertReq);
    break;
  }
  case GET_OR_INSERT_RESP: {
    GetOrInsertResp *resp = (GetOrInsertResp*)task;
    ret += sizeof(GetOrInsertResp);
    break;
  }
  case GET_POINTER_REQ: {
    GetPointerReq *req = (GetPointerReq*)task;
    // |key + hash_id|
    ret += req->len + sizeof(GetPointerReq);
    break;
  }
  case GET_POINTER_RESP: {
    GetPointerResp *resp= (GetPointerResp*)task;
    ret += sizeof(GetPointerResp);
    break;
  }
  case UPDATE_POINTER_REQ: {
    UpdatePointerReq *req = (UpdatePointerReq*)task;
    ret += sizeof(UpdatePointerReq);
    break;
  }
  case GET_MAX_LINK_SIZE_REQ: {
    GetMaxLinkSizeReq *req = (GetMaxLinkSizeReq*)task;
    ret += sizeof(GetMaxLinkSizeReq);
    break;
  }
  case GET_MAX_LINK_SIZE_RESP: {
    GetMaxLinkSizeResp *resp = (GetMaxLinkSizeResp*)task;
    ret += sizeof(GetMaxLinkSizeResp);
    break;
  }
  case FETCH_MAX_LINK_REQ: {
    FetchMaxLinkReq *req = (FetchMaxLinkReq*)task;
    ret += sizeof(FetchMaxLinkReq);
    break;
  }
  case FETCH_MAX_LINK_RESP: {
    FetchMaxLinkResp *resp = (FetchMaxLinkResp*)task;
    // add task type in the end, because we always remove it.
    ret += resp->maxLink.tupleIDCount * sizeof(TupleIdT) + resp->maxLink.hashAddrCount * sizeof(HashAddrT) + sizeof(MaxLinkT);
    break;
  }
  case MERGE_MAX_LINK_REQ: {
    MergeMaxLinkReq *req = (MergeMaxLinkReq*)task;
    // add task type in the end, because we always remove it.
    ret += req->maxLink.tupleIDCount * sizeof(TupleIdT) + req->maxLink.hashAddrCount * sizeof(HashAddrT) + sizeof(MaxLinkT);
    break;
  }
  default:
    break;
  }
  return ret;
}

