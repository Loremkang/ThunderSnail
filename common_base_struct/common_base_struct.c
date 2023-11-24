#include "common_base_struct.h"
#include <stdlib.h>
#include <inttypes.h>

inline void RemotePtrPrint(RemotePtrT rPtr) {
    printf("(RemotePtrT){.dpuId = %x\t, .dpuAddr = %x}\n", rPtr.dpuId, rPtr.dpuAddr);
}

void MaxLinkAddrPrint(MaxLinkAddrT maxLinkAddr) {
    printf("MaxLinkAddrT.rPtr = ");
    RemotePtrPrint(maxLinkAddr.rPtr);
}

void TupleIdPrint(TupleIdT tupleId) {
    printf("(TupleIdT){.tableId = %d\t, .tupleAddr = %" PRIx64 "}\n", tupleId.tableId, tupleId.tupleAddr);
}