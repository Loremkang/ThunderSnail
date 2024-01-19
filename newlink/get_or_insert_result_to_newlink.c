#include "get_or_insert_result_to_newlink.h"
#include <stdlib.h>
#include <time.h>
#include "shared_constants.h"

static inline void FillNewLinkBuffer(
    int* idxPos, bool* processed, DisjointSetNodeT* dsNode, int id,
    HashTableQueryReplyT value,
    VariableLengthStructBufferT* newLinkResultBuffer) {
  if (processed[id]) {
    return;
  }
  int rootId = DisjointSetFind(&dsNode[id]) - dsNode;
  if (idxPos[rootId] == -1) {
    int size = NewLinkGetSize(dsNode[rootId].tupleIdCount,
                              dsNode[rootId].maxLinkAddrCount,
                              dsNode[rootId].hashAddrCount);

    int idx =
        VariableLengthStructBufferAppendPlaceholder(newLinkResultBuffer, size);
    NewLinkT* nextNewLink =
        (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, idx);
    nextNewLink->tupleIDCount = dsNode[rootId].tupleIdCount;
    nextNewLink->maxLinkAddrCount = dsNode[rootId].maxLinkAddrCount;
    nextNewLink->hashAddrCount = dsNode[rootId].hashAddrCount;
    idxPos[rootId] = idx;
  }
  int idx = idxPos[rootId];
  NewLinkT* newLink =
      (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, idx);
  // printf("Insert to NewLink %d[%d/%d]: ", idx, newLink->tupleIDCount - dsNode[rootId].tupleIdCount, newLink->maxLinkAddrCount - dsNode[rootId].maxLinkAddrCount);
  switch (value.type) {
    case TupleId:
      // TupleIdPrint(value.value.tupleId);
      ValidValueCheck(dsNode[rootId].tupleIdCount > 0);
      NewLinkGetTupleIDs(newLink)[--dsNode[rootId].tupleIdCount] =
          value.value.tupleId;
      break;
    case MaxLinkAddr:
      // MaxLinkAddrPrint(value.value.maxLinkAddr);
      ValidValueCheck(dsNode[rootId].maxLinkAddrCount > 0);
      NewLinkGetMaxLinkAddrs(newLink)[--dsNode[rootId].maxLinkAddrCount] =
          value.value.maxLinkAddr;
      break;
    case HashAddr:
      ValidValueCheck(dsNode[rootId].hashAddrCount > 0);
      NewLinkGetHashAddrs(newLink)[--dsNode[rootId].hashAddrCount] =
          value.value.hashAddr;
      break;
    default:
      ValidValueCheck(0);
  }
  processed[id] = true;
}

static inline bool ValidHashTableQueryReply(HashTableQueryReplyT reply) {
  switch (reply.type) {
    case TupleId:
      return reply.value.tupleId.tableId > 0 &&
             (void*)reply.value.tupleId.tupleAddr != NULL;
    case MaxLinkAddr:
      return !RemotePtrInvalid(reply.value.maxLinkAddr.rPtr);
    case HashAddr:
      return !RemotePtrInvalid(reply.value.hashAddr.rPtr);
    default:
      ValidValueCheck(0);
  }
}

static inline void BuildIds(int length, HashTableForNewLinkT* ht, int* leftIds,
                            int* rightIds, TupleIdT* tupleIDs,
                            HashTableQueryReplyT* counterpart) {
  assert(length * 2 < MAXSIZE_HASH_TABLE_QUERY_BATCH);
  HashTableForNewLinkExpandAndSoftReset(ht, length * 2 * 1.5);
  for (int i = 0; i < length; i++) {
    HashTableQueryReplyT left =
        (HashTableQueryReplyT){.type = TupleId, .value.tupleId = tupleIDs[i]};
    int leftId = HashTableForNewlinkGetId(ht, left);
    leftIds[i] = leftId;
    HashTableQueryReplyT right = counterpart[i];
    ValidValueCheck(ValidHashTableQueryReply(right));
    rightIds[i] = HashTableForNewlinkGetId(ht, right);
  }
}

void BuildNewLinkFromHashTableGetOrInsertResult(
    int length, int* idxPos, bool* processed, DisjointSetNodeT* dsNode,
    int* leftIds, int* rightIds, TupleIdT* tupleIDs,
    HashTableQueryReplyT* counterpart,
    VariableLengthStructBufferT* newLinkResultBuffer) {
  // HashTableForNewlinkGetId reply starts from 1, not 0. So use "<" here, not
  // "<=".
  // "* 2" for left and right
  assert(length * 2 < MAXSIZE_HASH_TABLE_QUERY_BATCH);
  memset(idxPos, -1, (2 * length + 1) * sizeof(int));

  // Disjoint Set Processing
  for (int i = 0; i < length; i++) {
    int leftId = leftIds[i];
    int rightId = rightIds[i];
    DisjointSetInit(&dsNode[leftId]);
    dsNode[leftId].tupleIdCount = 1;
    if (rightId == 0) {
      continue;
    }
    DisjointSetInit(&dsNode[rightId]);
    switch (counterpart[i].type) {
      case TupleId:
        dsNode[rightId].tupleIdCount = 1;
        break;
      case MaxLinkAddr:
        dsNode[rightId].maxLinkAddrCount = 1;
        break;
      case HashAddr:
        dsNode[rightId].hashAddrCount = 1;
        break;
      default:
        ValidValueCheck(0);
    }
  }
  for (int i = 0; i < length; i++) {
    int leftId = leftIds[i];
    int rightId = rightIds[i];
    DisjointSetJoin(&dsNode[leftId], &dsNode[rightId]);
  }

  // Fill the NewLink Buffer
  memset(processed, 0, (2 * length + 1) * sizeof(bool));
  VariableLengthStructBufferSoftReset(newLinkResultBuffer);
  // for (int i = 1; i <= length * 2; i ++) {
  //     printf("rootId[%d] = %ld\n", i, DisjointSetFind(&dsNode[i]) -
  //     dsNode);
  // }
  for (int i = 0; i < length; i++) {
    HashTableQueryReplyT left =
        (HashTableQueryReplyT){.type = TupleId, .value.tupleId = tupleIDs[i]};
    HashTableQueryReplyT right = counterpart[i];
    FillNewLinkBuffer(idxPos, processed, dsNode, leftIds[i], left,
                      newLinkResultBuffer);
    ValidValueCheck(ValidHashTableQueryReply(right));
    FillNewLinkBuffer(idxPos, processed, dsNode, rightIds[i], right,
                      newLinkResultBuffer);
  }

  // print result
  // for (int i = 0; i < newLinkResultBuffer->count; i ++) {
  //     NewLinkT* newLink =
  //     (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, i);
  //     NewLinkPrint(newLink);
  // }
}

// performance counter
static inline double get_timestamp() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec + ts.tv_nsec / 1e9;
}

void GetOrInsertResultToNewlink(int length, TupleIdT* tupleIds,
                                HashTableQueryReplyT* counterpart,
                                HashTableForNewLinkT* ht,
                                VariableLengthStructBufferT* buf) {
  static DisjointSetNodeT dsNode[MAXSIZE_HASH_TABLE_QUERY_BATCH];
  static int idxPos[MAXSIZE_HASH_TABLE_QUERY_BATCH];
  static bool processed[MAXSIZE_HASH_TABLE_QUERY_BATCH];
  static int leftIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
  static int rightIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
  BuildIds(length, ht, leftIds, rightIds, tupleIds, counterpart);
  BuildNewLinkFromHashTableGetOrInsertResult(length, idxPos, processed, dsNode,
                                             leftIds, rightIds, tupleIds,
                                             counterpart, buf);
}

void PrintNewLinkResult(VariableLengthStructBufferT* buf) {
  for (int i = 0; i < buf->count; i++) {
    NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(buf, i);
    NewLinkPrint(newLink);
    printf("\n");
  }
}

// TODO: careful consideration on memory management
void BuildNewLinkFromHashTableGetOrInsertResultPerformanceTest() {
  static HashTableForNewLinkT ht;
  HashTableForNewLinkInit(&ht);
  static VariableLengthStructBufferT buf;
  VariableLengthStructBufferInit(&buf);

  const int testbatchsize = 400;
  TupleIdT tupleIds[MAXSIZE_HASH_TABLE_QUERY_BATCH];
  HashTableQueryReplyT counterpart[MAXSIZE_HASH_TABLE_QUERY_BATCH];

  // TESTCASE: CHAIN_NATURAL_JOIN(T0, T1, ..., T9, T10)
  // Keep Empty: T0
  // Existing: T10
  // Inserting: T1, ..., T9

  const int NR_TABLES = 9;
  for (int T = 1; T <= NR_TABLES; T++) {
    int offset = testbatchsize * (T - 1) * 2;
    for (int i = 0; i < testbatchsize; i++) {  // T to T-1
      int pos = i + offset;
      tupleIds[pos] = (TupleIdT){.tableId = T, .tupleAddr = i + 1};
      if (T == 1) {
        counterpart[pos].type = HashAddr;
        counterpart[pos].value.hashAddr.rPtr =
            (RemotePtrT){.dpuId = 0, .dpuAddr = i + testbatchsize};
      } else {
        counterpart[pos].type = TupleId;
        counterpart[pos].value.tupleId =
            (TupleIdT){.tableId = T - 1, .tupleAddr = i + 1};
      }
    }
    offset += testbatchsize;
    for (int i = 0; i < testbatchsize; i++) {  // T to T+1
      int pos = i + offset;
      tupleIds[pos] = (TupleIdT){.tableId = T, .tupleAddr = i + 1};
      if (T == 9) {
        counterpart[pos].type = MaxLinkAddr;
        counterpart[pos].value.maxLinkAddr.rPtr =
            (RemotePtrT){.dpuId = T + 1, .dpuAddr = i};
      } else {
        counterpart[pos].type = HashAddr;
        counterpart[pos].value.hashAddr.rPtr =
            (RemotePtrT){.dpuId = T + 1, .dpuAddr = i + testbatchsize};
      }
    }
  }

  int length = NR_TABLES * 2 * testbatchsize;
  // BuildIds(length, &ht, leftIds, rightIds, tupleIds, counterpart);
  // BuildNewLinkFromHashTableGetOrInsertResult(
  //     length, idxPos, processed, dsNode, leftIds, rightIds,
  //     tupleIds, counterpart, &buf);

  int testRound = 1000;
  double timeStart = get_timestamp();
  for (int i = 0; i < testRound; i++) {
    GetOrInsertResultToNewlink(length, tupleIds, counterpart, &ht, &buf);
  }
  double timeSpent = get_timestamp() - timeStart;
  printf("Number of Rounds: %d\n", testRound);
  printf("Time Per Task: %lf ns\n",
         timeSpent / testRound * 1e9 / NR_TABLES / testbatchsize);

  VariableLengthStructBufferFree(&buf);
  HashTableForNewLinkFree(&ht);
}