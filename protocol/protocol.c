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
    ret += req->len + sizeof(TupleIdT) + sizeof(HashTableId);
    break;
  }
  case GET_POINTER_REQ: {
    GetPointerReq *req = (GetPointerReq*)task;
    // |key + hash_id|
    ret += req->len + sizeof(HashTableId);
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
  case FETCH_MAX_LINK_REQ: {
    FetchMaxLinkSizeReq *req = (FetchMaxLinkSizeReq*)task;
    ret += sizeof(FetchMaxLinkReq);
    break;
  }
  case MERGE_MAX_LINK_REQ: {
    MergeMaxLinkReq *req = (MergeMaxLinkReq*)task;
    ret += req->maxLink.tupleIdCount + req->maxLink.hashAddrCount + sizeof(int)*2;
    break;
  }
  default:
    break;
  }
  return ret;
}

uint16_t GetVarLenTaskSize(void *task)
{
  return GetFixedLenTaskSize(task) + sizeof(Offset);
}

