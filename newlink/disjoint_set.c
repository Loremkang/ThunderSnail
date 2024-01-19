#include "disjoint_set.h"
#include "safety_check_macro.h"

void DisjointSetInit(DisjointSetNodeT* node) {
  node->parent = node;
  node->maxLinkAddrCount = node->tupleIdCount = node->hashAddrCount = 0;
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
    DisjointSetFind(b)->maxLinkAddrCount +=
        DisjointSetFind(a)->maxLinkAddrCount;
    DisjointSetFind(b)->hashAddrCount += DisjointSetFind(a)->hashAddrCount;
    DisjointSetFind(a)->parent = DisjointSetFind(b);
  }
}