#include "get_or_insert_result_to_newlink.h"
#include <time.h>
#include <stdlib.h>

#define MAXBATCHSIZE (24000)

static inline void FillNewLinkBuffer(int* idxPos, bool* processed, DisjointSetNodeT* dsNode,
                       int id, TupleIdOrMaxLinkAddrT value,
                       VariableLengthStructBufferT* newLinkResultBuffer) {
    if (processed[id]) {
        return;
    }
    int rootId = DisjointSetFind(&dsNode[id]) - dsNode;
    if (idxPos[rootId] == -1) {
        int size = NewLinkGetSize(dsNode[rootId].tupleIdCount,
                                  dsNode[rootId].maxLinkAddrCount);

        int idx = VariableLengthStructBufferAppendPlaceholder(
            newLinkResultBuffer, size);
        NewLinkT* nextNewLink =
            (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, idx);
        nextNewLink->tupleIDCount = dsNode[rootId].tupleIdCount;
        nextNewLink->maxLinkAddrCount = dsNode[rootId].maxLinkAddrCount;
        idxPos[rootId] = idx;
    }
    int idx = idxPos[rootId];
    NewLinkT* newLink =
        (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, idx);
    // printf("Insert to NewLink %d[%d/%d]: ", idx, newLink->tupleIDCount - dsNode[rootId].tupleIdCount, newLink->maxLinkAddrCount - dsNode[rootId].maxLinkAddrCount);
    if (value.type == TupleId) {
        // TupleIdPrint(value.value.tupleId);
        ValidValueCheck(dsNode[rootId].tupleIdCount > 0);
        NewLinkGetTupleIDs(newLink)[--dsNode[rootId].tupleIdCount] =
            value.value.tupleId;
    } else {
        ValidValueCheck(value.type == MaxLinkAddr);
        // MaxLinkAddrPrint(value.value.maxLinkAddr);
        ValidValueCheck(dsNode[rootId].maxLinkAddrCount > 0);
        NewLinkGetMaxLinkAddrs(newLink)[--dsNode[rootId].maxLinkAddrCount] = value.value.maxLinkAddr;
    }
    processed[id] = true;
}

static inline void BuildIds(int length, HashTableForNewLinkT* ht, int* leftIds,
                            int* rightIds, TupleIdT* tupleIDs,
                            TupleIdOrMaxLinkAddrT* counterpart) {
    assert(length * 2 < MAXBATCHSIZE);
    HashTableForNewLinkExpandAndSoftReset(ht, length * 2 * 1.5);
    for (int i = 0; i < length; i++) {
        TupleIdOrMaxLinkAddrT left = (TupleIdOrMaxLinkAddrT){
            .type = TupleId, .value.tupleId = tupleIDs[i]};
        int leftId = HashTableForNewlinkGetId(ht, left);
        leftIds[i] = leftId;
        TupleIdOrMaxLinkAddrT right = counterpart[i];
        if (right.type == MaxLinkAddr &&
            RemotePtrInvalid(right.value.maxLinkAddr.rPtr)) {
            rightIds[i] = 0;
            continue;
        } else {
            rightIds[i] = HashTableForNewlinkGetId(ht, right);
        }
    }
}

void BuildNewLinkFromHashTableGetOrInsertResult(
    int length, int* idxPos, bool* processed, DisjointSetNodeT* dsNode,
    int* leftIds, int* rightIds, TupleIdT* tupleIDs,
    TupleIdOrMaxLinkAddrT* counterpart,
    VariableLengthStructBufferT* newLinkResultBuffer) {
    // HashTableForNewlinkGetId reply starts from 1, not 0. So use "<" here, not
    // "<=".
    // "* 2" for left and right
    assert(length * 2 < MAXBATCHSIZE);
    memset(idxPos, -1, (2 * length + 1) * sizeof(int));

    // Disjoint Set Processing
    for (int i = 0; i < length; i ++) {
        int leftId = leftIds[i];
        int rightId = rightIds[i];
        DisjointSetInit(&dsNode[leftId]);
        dsNode[leftId].tupleIdCount = 1;
        if (rightId == 0) {
            continue;
        }
        DisjointSetInit(&dsNode[rightId]);
        if (counterpart[i].type == TupleId) {
            dsNode[rightId].tupleIdCount = 1;
        } else {
            dsNode[rightId].maxLinkAddrCount = 1;
        }
    }
    for (int i = 0; i < length; i++) {
        int leftId = leftIds[i];
        int rightId = rightIds[i];
        if (rightId == 0) {
            continue;
        }
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
        TupleIdOrMaxLinkAddrT left = (TupleIdOrMaxLinkAddrT){
            .type = TupleId, .value.tupleId = tupleIDs[i]};
        TupleIdOrMaxLinkAddrT right = counterpart[i];
        FillNewLinkBuffer(idxPos, processed, dsNode, leftIds[i], left,
                          newLinkResultBuffer);
        if (right.type == MaxLinkAddr &&
            RemotePtrInvalid(right.value.maxLinkAddr.rPtr)) {
            continue;
        }
        FillNewLinkBuffer(idxPos, processed, dsNode, rightIds[i], right,
                          newLinkResultBuffer);
    }

    // correctness check
//     for (int i = 0; i < newLinkResultBuffer->count; i ++) {
//         NewLinkT* newLink =
//         (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, i);
//         NewLinkPrint(newLink);
//     }
}

// need careful consideration on memory management. ignored now.
void BuildNewLinkFromHashTableGetOrInsertResultTest() {
    static DisjointSetNodeT dsNode[MAXBATCHSIZE];
    static int idxPos[MAXBATCHSIZE];
    static bool processed[MAXBATCHSIZE];
    static int leftIds[MAXBATCHSIZE];
    static int rightIds[MAXBATCHSIZE];
    static HashTableForNewLinkT ht;
    HashTableForNewLinkInit(&ht);
    static VariableLengthStructBufferT buf;
    VariableLengthStructBufferInit(&buf);

    const int testbatchsize = 30;
    TupleIdT tupleIds[MAXBATCHSIZE];
    TupleIdOrMaxLinkAddrT counterpart[MAXBATCHSIZE];

    // TESTCASE: NATURAL_JOIN(A, B, C, D)
    // Existing: C, D
    // Inserting: A(tableid=0), B(tableid=1)

    for (int i = 0; i < testbatchsize; i++) {  // table A
        tupleIds[i] = (TupleIdT){.tableId = 0, .tupleAddr = i};
        counterpart[i].type = MaxLinkAddr;
        counterpart[i].value.maxLinkAddr.rPtr = INVALID_REMOTEPTR;
    }
    for (int i = 0; i < testbatchsize; i++) {  // table B->A
        tupleIds[i + testbatchsize] = (TupleIdT){.tableId = 1, .tupleAddr = i};
        counterpart[i + testbatchsize].type = TupleId;
        counterpart[i + testbatchsize].value.tupleId = tupleIds[i];
    }
    for (int i = 0; i < testbatchsize; i++) {  // table B->C
        tupleIds[i + 2 * testbatchsize] =
            (TupleIdT){.tableId = 1, .tupleAddr = i};
        counterpart[i + 2 * testbatchsize].type = MaxLinkAddr;
        counterpart[i + 2 * testbatchsize].value.maxLinkAddr.rPtr =
            (RemotePtrT){.dpuId = 1, .dpuAddr = i};
    }

    int length = 3 * testbatchsize;
    BuildIds(length, &ht, leftIds, rightIds, tupleIds, counterpart);
    BuildNewLinkFromHashTableGetOrInsertResult(
        length, idxPos, processed, dsNode, leftIds, rightIds,
        tupleIds, counterpart, &buf);
    VariableLengthStructBufferFree(&buf);
    HashTableForNewLinkFree(&ht);
}

// performance counter
static inline double get_timestamp() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

void BuildNewLinkFromHashTableGetOrInsertResultPerformanceTest() {
    static DisjointSetNodeT dsNode[MAXBATCHSIZE];
    static int idxPos[MAXBATCHSIZE];
    static bool processed[MAXBATCHSIZE];
    static int leftIds[MAXBATCHSIZE];
    static int rightIds[MAXBATCHSIZE];
    static HashTableForNewLinkT ht;
    HashTableForNewLinkInit(&ht);
    static VariableLengthStructBufferT buf;
    VariableLengthStructBufferInit(&buf);

    const int testbatchsize = 400;
    TupleIdT tupleIds[MAXBATCHSIZE];
    TupleIdOrMaxLinkAddrT counterpart[MAXBATCHSIZE];

    // TESTCASE: CHAIN_NATURAL_JOIN(T0, T1, ..., T9, T10)
    // Existing: T0, T10
    // Inserting: T1, ..., T9

    const int NR_TABLES = 9;
    for (int T = 1; T <= NR_TABLES; T++) {
        int offset = testbatchsize * (T - 1) * 2;
        for (int i = 0; i < testbatchsize; i ++) { // T to T-1
            int pos = i + offset;
            tupleIds[pos] = (TupleIdT){.tableId = T, .tupleAddr = i};
            if (T == 1) {
                counterpart[pos].type = MaxLinkAddr;
                counterpart[pos].value.maxLinkAddr.rPtr =
                    (RemotePtrT){.dpuId = 0, .dpuAddr = i};
            } else {
                counterpart[pos].type = TupleId;
                counterpart[pos].value.tupleId =
                    (TupleIdT){.tableId = T - 1, .tupleAddr = i};
            }
        }
        offset += testbatchsize;
        for (int i = 0; i < testbatchsize; i++) { // T to T+1
            int pos = i + offset;
            tupleIds[pos] = (TupleIdT){.tableId = T, .tupleAddr = i};
            if (T == 9) {
                counterpart[pos].type = MaxLinkAddr;
                counterpart[pos].value.maxLinkAddr.rPtr =
                    (RemotePtrT){.dpuId = T + 1, .dpuAddr = i};
            } else {
                counterpart[pos].type = MaxLinkAddr;
                counterpart[pos].value.maxLinkAddr.rPtr = INVALID_REMOTEPTR;
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
        BuildIds(length, &ht, leftIds, rightIds, tupleIds, counterpart);
        BuildNewLinkFromHashTableGetOrInsertResult(
            length, idxPos, processed, dsNode, leftIds, rightIds,
            tupleIds, counterpart, &buf);
    }
    double timeSpent = get_timestamp() - timeStart;
    printf("Time Per Task: %lf ns\n",
           timeSpent / testRound * 1e9 / NR_TABLES / testbatchsize);

    VariableLengthStructBufferFree(&buf);
    HashTableForNewLinkFree(&ht);
}