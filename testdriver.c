#include "stdio.h"
#include "common_base_struct/common_base_struct.h"
#include "newlink/disjoint_set.h"

int main() {
    assert(GetMaxLinkSize(5, 5) == 128);
    assert(DisjointSetTest());
    return 0;
}