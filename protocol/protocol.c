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
  case SET_DPU_ID_REQ: {
    ret += sizeof(SetDpuIdReq);
    break;
  }
  case CREATE_INDEX_REQ: {
    ret += sizeof(CreateIndexReq);
    break;
  }
  case GET_OR_INSERT_REQ: {
    GetOrInsertReq *req = (GetOrInsertReq*)task;
    // |key + value + hash_id|
    ret += ROUND_UP_TO_8(req->len) + sizeof(GetOrInsertReq);
    break;
  }
  case GET_OR_INSERT_RESP: {
    ret += sizeof(GetOrInsertResp);
    break;
  }
  case GET_POINTER_REQ: {
    GetPointerReq *req = (GetPointerReq*)task;
    // |key + hash_id|
    ret += ROUND_UP_TO_8(req->len) + sizeof(GetPointerReq);
    break;
  }
  case GET_POINTER_RESP: {
    ret += sizeof(GetPointerResp);
    break;
  }
  case UPDATE_POINTER_REQ: {
    ret += sizeof(UpdatePointerReq);
    break;
  }
  case GET_MAX_LINK_SIZE_REQ: {
    ret += sizeof(GetMaxLinkSizeReq);
    break;
  }
  case GET_MAX_LINK_SIZE_RESP: {
    ret += sizeof(GetMaxLinkSizeResp);
    break;
  }
  case FETCH_MAX_LINK_REQ: {
    ret += sizeof(FetchMaxLinkReq);
    break;
  }
  case FETCH_MAX_LINK_RESP: {
    FetchMaxLinkResp *resp = (FetchMaxLinkResp*)task;
    // add task type in the end, because we always remove it.
    ret += ROUND_UP_TO_8(resp->maxLink.tupleIDCount * sizeof(TupleIdT) + resp->maxLink.hashAddrCount * sizeof(HashAddrT)) + sizeof(FetchMaxLinkResp);
    break;
  }
  case MERGE_MAX_LINK_REQ: {
    MergeMaxLinkReq *req = (MergeMaxLinkReq*)task;
    // add task type in the end, because we always remove it.
    ret += ROUND_UP_TO_8(req->maxLink.tupleIDCount * sizeof(TupleIdT) +
                         req->maxLink.hashAddrCount * sizeof(HashAddrT) +
                         sizeof(MergeMaxLinkReq));
    break;
  }
  case NEW_MAX_LINK_REQ: {
    NewMaxLinkReq *req = (NewMaxLinkReq*)task;
    ret += ROUND_UP_TO_8(req->maxLink.tupleIDCount * sizeof(TupleIdT) +
                         req->maxLink.hashAddrCount * sizeof(HashAddrT) +
                         sizeof(NewMaxLinkReq));
    break;
  }
  case NEW_MAX_LINK_RESP: {
    ret += sizeof(NewMaxLinkResp);
  }
  default:
    ValidValueCheck(0);
    break;
  }
  return ret;
}

