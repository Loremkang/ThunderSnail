#include "stdio.h"
#include "common_base_struct/common_base_struct.h"

int main() {
    assert(GetMaxLinkSize(5, 5) == 128);
    return 0;
}