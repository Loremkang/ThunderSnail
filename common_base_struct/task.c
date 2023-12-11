
#include "task.h"
#include "common_base_struct.h"

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
  const int NAME##_id = (ID); \
  const bool NAME##_fixed = (FIXED); \
  const int NAME##_task_len = (LEN);

#include "task_def.h"

uint8_t RespTaskType(uint8_t taskType) {
  switch(taskType) {
  case GET_OR_INSERT_REQ:
    return GET_OR_INSERT_RESP;
  case GET_POINTER_REQ:
    return GET_POINTER_RESP;
  case UPDATE_POINTER_REQ:
    return UPDATE_POINTER_RESP;
  case GET_MAX_LINK_SIZE_REQ:
    return GET_MAX_LINK_SIZE_RESP;
  case FETCH_MAX_LINK_REQ:
    return FETCH_MAX_LINK_RESP;
  case MERGE_MAX_LINK_REQ:
    return MERGE_MAX_LINK_RESP;
  case SET_DPU_ID_REQ:
  case CREATE_INDEX_REQ:
    return EMPTY_RESP;
  default:
    ValidValueCheck(0);
    return -1;
  }
}

bool IsVarLenTask(uint8_t taskType)
{
  switch(taskType) {
    case GET_POINTER_REQ:
    case GET_OR_INSERT_REQ:
    case GET_MAX_LINK_SIZE_REQ:
    case MERGE_MAX_LINK_REQ:
    case FETCH_MAX_LINK_RESP:
      return true;
    default:
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

