#include "disjoint_set.h"
#include "safety_check_macro.h"

void DisjointSetInit(DisjointSetNodeT* node) {
    node->parent = node;
    node->maxLinkAddrCount = node->tupleIdCount = 0;
}

DisjointSetNodeT* DisjointSetFind(DisjointSetNodeT* node) {
    NullPointerCheck(node != NULL);
    if (node->parent == node) {
        return node;
    } else {
        node->parent = DisjointSetFind(node->parent);
        return node->parent;
    }
}

void DisjointSetJoin(DisjointSetNodeT* a, DisjointSetNodeT* b) {
    if (DisjointSetFind(a) != DisjointSetFind(b)) {
        DisjointSetFind(b)->tupleIdCount += DisjointSetFind(a)->tupleIdCount;
        DisjointSetFind(b)->maxLinkAddrCount += DisjointSetFind(a)->maxLinkAddrCount;
        DisjointSetFind(a)->parent = DisjointSetFind(b);
    }
}

// #define TEST_FAIL(x) if(!(x)) {goto fail;}
#define TEST_FAIL(x) assert(x);

// simple test code
int DisjointSetTest() {
    const int MAXN = 100;
    DisjointSetNodeT node[MAXN];
    for (int i = 0; i < MAXN; i ++) {
        DisjointSetInit(&node[i]);
        node[i].tupleIdCount = 1;
        node[i].maxLinkAddrCount = i;
    }
    for (int i = 0; i < MAXN; i ++) {
        TEST_FAIL(DisjointSetFind(&node[i]) == &node[i]);
        DisjointSetJoin(&node[i], &node[0]);
        if (i > 0) {
            TEST_FAIL(DisjointSetFind(&node[i]) == DisjointSetFind(&node[i - 1]));
        }
        TEST_FAIL(DisjointSetFind(&node[i])->tupleIdCount == i + 1);
        TEST_FAIL(DisjointSetFind(&node[i])->maxLinkAddrCount == (i * (i + 1) / 2));
    }
    return 1;

fail:
    return 0;
}