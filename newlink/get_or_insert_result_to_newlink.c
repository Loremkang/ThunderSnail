#include "get_or_insert_result_to_newlink.h"

const int MAXBATCHSIZE = 24000;

static inline void UpdateDSNode(TupleIdOrMaxLinkAddrT value, DisjointSetNodeT* node) {
    if (value.type == TupleId) {
        node->tupleIdCount ++;
    } else {
        ValidValueCheck(value.type == MaxLinkAddr);
        node->maxLinkAddrCount ++;
    }
}

static inline void FillNewLinkBuffer(int* idxPos, bool* processed, DisjointSetNodeT* dsNode,
                       HashTableForNewLinkT* ht, TupleIdOrMaxLinkAddrT value,
                       VariableLengthStructBufferT* newLinkResultBuffer) {
    int id = HashTableForNewlinkGetId(ht, value);
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

void BuildNewLinkFromHashTableGetOrInsertResult(
    int length, int* idxPos, bool* processed, DisjointSetNodeT* dsNode,
    HashTableForNewLinkT* ht, TupleIdT* tupleIDs,
    TupleIdOrMaxLinkAddrT* counterpart,
    VariableLengthStructBufferT* newLinkResultBuffer) {
    // HashTableForNewlinkGetId reply starts from 1, not 0. So use "<" here, not "<=".
    // * 2 for left and right
    assert(length * 2 < MAXBATCHSIZE);

    for (int i = 0; i <= length * 2; i++) {
        DisjointSetInit(&dsNode[i]);
        processed[i] = false;
        idxPos[i] = -1;
    }
    HashTableForNewLinkExpand(ht, length * 2);
    VariableLengthStructBufferSoftReset(newLinkResultBuffer);

    for (int i = 0; i < length; i++) {
        TupleIdOrMaxLinkAddrT left = (TupleIdOrMaxLinkAddrT){
            .type = TupleId, .value.tupleId = tupleIDs[i]};
        TupleIdOrMaxLinkAddrT right = counterpart[i];
        if (right.type == MaxLinkAddr && RemotePtrInvalid(right.value.maxLinkAddr.rPtr)) {
            continue;
        }
        int leftId = HashTableForNewlinkGetId(ht, left);
        int rightId = HashTableForNewlinkGetId(ht, right);
        if (!processed[leftId]) {
            ValidValueCheck(left.type == TupleId);
            processed[leftId] = true;
            UpdateDSNode(left, &dsNode[leftId]);
        }
        if (!processed[rightId]) {
            processed[rightId] = true;
            UpdateDSNode(right, &dsNode[rightId]);
        }
        
        // TupleIdPrint(left.value.tupleId);
        // printf("lid=%d\trid=%d\n", leftId, rightId);
        DisjointSetJoin(&dsNode[leftId], &dsNode[rightId]);
    }
    memset(processed, 0, (2 * length + 1) * sizeof(bool));
    // for (int i = 1; i <= length * 2; i ++) {
    //     printf("rootId[%d] = %ld\n", i, DisjointSetFind(&dsNode[i]) - dsNode);
    // }
    for (int i = 0; i < length; i++) {
        TupleIdOrMaxLinkAddrT left = (TupleIdOrMaxLinkAddrT){
            .type = TupleId, .value.tupleId = tupleIDs[i]};
        TupleIdOrMaxLinkAddrT right = counterpart[i];
        FillNewLinkBuffer(idxPos, processed, dsNode, ht, left, newLinkResultBuffer);
        if (right.type == MaxLinkAddr && RemotePtrInvalid(right.value.maxLinkAddr.rPtr)) {
            continue;
        }
        FillNewLinkBuffer(idxPos, processed, dsNode, ht, right, newLinkResultBuffer);
    }

    // correctness check
    // for (int i = 0; i < newLinkResultBuffer->count; i ++) {
    //     NewLinkT* newLink = (NewLinkT*)VariableLengthStructBufferGet(newLinkResultBuffer, i);
    //     NewLinkPrint(newLink);
    // }
}

// entry of this part of code
// need careful consideration on memory management. ignored now.

void BuildNewLinkFromHashTableGetOrInsertResultTest() {
    static DisjointSetNodeT dsNode[MAXBATCHSIZE];
    static int idxPos[MAXBATCHSIZE];
    static bool processed[MAXBATCHSIZE];
    static HashTableForNewLinkT ht;
    HashTableForNewLinkInit(&ht);
    static VariableLengthStructBufferT buf;
    VariableLengthStructBufferInit(&buf);

    const int testbatchsize = 3600;
    TupleIdT tupleIds[MAXBATCHSIZE];
    TupleIdOrMaxLinkAddrT counterpart[MAXBATCHSIZE];

    // TESTCASE: NATURAL_JOIN(A, B, C, D)
    // Existing: C, D
    // Inserting: A(tableid=0), B(tableid=1)

    for (int i = 0; i < testbatchsize; i ++) { // table A
        tupleIds[i] = (TupleIdT){.tableId = 0, .tupleAddr = i};
        counterpart[i].type = MaxLinkAddr;
        counterpart[i].value.maxLinkAddr.rPtr = INVALID_REMOTEPTR;
    }
    for (int i = 0; i < testbatchsize; i ++) { // table B->A
        tupleIds[i + testbatchsize] = (TupleIdT){.tableId = 1, .tupleAddr = i};
        counterpart[i + testbatchsize].type = TupleId;
        counterpart[i + testbatchsize].value.tupleId = tupleIds[i];
    }
    for (int i = 0; i < testbatchsize; i ++) { // table B->C
        tupleIds[i + 2 * testbatchsize] = (TupleIdT){.tableId = 1, .tupleAddr = i};
        counterpart[i + 2 * testbatchsize].type = MaxLinkAddr;
        counterpart[i + 2 * testbatchsize].value.maxLinkAddr.rPtr = (RemotePtrT){.dpuId = 1, .dpuAddr = i};
    }

    BuildNewLinkFromHashTableGetOrInsertResult(3 * testbatchsize, idxPos, processed, dsNode, &ht, tupleIds, counterpart, &buf);
    VariableLengthStructBufferFree(&buf);
}