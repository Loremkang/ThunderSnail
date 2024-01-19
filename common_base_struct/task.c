
#include "task.h"
#include "common_base_struct.h"

#define TASK(NAME, ID, FIXED, LEN, CONTENT) \
  const int NAME##_id = (ID);               \
  const bool NAME##_fixed = (FIXED);        \
  const int NAME##_task_len = (LEN);

#include "task_def.h"

uint8_t RespTaskType(uint8_t taskType) {
  switch (taskType) {
    case SET_DPU_ID_REQ:
    case CREATE_INDEX_REQ:
      return EMPTY_RESP;
    default:
      return taskType + 127;
  }
}

bool IsVarLenTask(uint8_t taskType) {
  switch (taskType) {
    case GET_POINTER_REQ:
    case GET_OR_INSERT_REQ:
    case FETCH_MAX_LINK_RESP:
    case MERGE_MAX_LINK_REQ:
    case NEW_MAX_LINK_REQ:
      return true;
    default:
      return false;
  }
}

uint16_t GetTaskLen(void* task) {
  // FIXME: complete other tasks
  uint8_t taskType = ((Task*)task)->taskType;
  uint16_t ret = 0;

  switch (taskType) {
    case SET_DPU_ID_REQ: {
      return sizeof(SetDpuIdReq);
    }
    case CREATE_INDEX_REQ: {
      return sizeof(CreateIndexReq);
    }
    case GET_OR_INSERT_REQ: {
      GetOrInsertReq* req = (GetOrInsertReq*)task;
      // |key + value + hash_id|
      return ROUND_UP_TO_8(req->len) + sizeof(GetOrInsertReq);
    }
    case GET_OR_INSERT_RESP: {
      return sizeof(GetOrInsertResp);
    }
    case GET_POINTER_REQ: {
      GetPointerReq* req = (GetPointerReq*)task;
      // |key + hash_id|
      return ROUND_UP_TO_8(req->len) + sizeof(GetPointerReq);
    }
    case GET_POINTER_RESP: {
      return sizeof(GetPointerResp);
    }
    case UPDATE_POINTER_REQ: {
      return sizeof(UpdatePointerReq);
    }
    case GET_MAX_LINK_SIZE_REQ: {
      return sizeof(GetMaxLinkSizeReq);
    }
    case GET_MAX_LINK_SIZE_RESP: {
      return sizeof(GetMaxLinkSizeResp);
    }
    case FETCH_MAX_LINK_REQ: {
      return sizeof(FetchMaxLinkReq);
    }
    case FETCH_MAX_LINK_RESP: {
      FetchMaxLinkResp* resp = (FetchMaxLinkResp*)task;
      // add task type in the end, because we always remove it.
      return ROUND_UP_TO_8(resp->maxLink.tupleIDCount * sizeof(TupleIdT) +
                           resp->maxLink.hashAddrCount * sizeof(HashAddrT)) +
             sizeof(FetchMaxLinkResp);
    }
    case MERGE_MAX_LINK_REQ: {
      MergeMaxLinkReq* req = (MergeMaxLinkReq*)task;
      // add task type in the end, because we always remove it.
      return ROUND_UP_TO_8(req->maxLink.tupleIDCount * sizeof(TupleIdT) +
                           req->maxLink.hashAddrCount * sizeof(HashAddrT) +
                           sizeof(MergeMaxLinkReq));
    }
    case NEW_MAX_LINK_REQ: {
      NewMaxLinkReq* req = (NewMaxLinkReq*)task;
      return ROUND_UP_TO_8(req->maxLink.tupleIDCount * sizeof(TupleIdT) +
                           req->maxLink.hashAddrCount * sizeof(HashAddrT) +
                           sizeof(NewMaxLinkReq));
      break;
    }
    case NEW_MAX_LINK_RESP: {
      return sizeof(NewMaxLinkResp);
    }
    case GET_VALID_MAXLINK_COUNT_REQ: {
      return sizeof(GetValidMaxLinkCountReq);
    }
    case GET_VALID_MAXLINK_COUNT_RESP: {
      return sizeof(GetValidMaxLinkCountResp);
    }
    default:
      ValidValueCheck(0);
      break;
  }
  return ret;
}
