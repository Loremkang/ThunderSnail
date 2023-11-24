#include "common_base_struct.h"
#include <stdlib.h>

// RemotePtr Related Function

inline bool RemotePtrAndUint64Test() {
    RemotePtrT x = (RemotePtrT){.dpuId = rand(), .dpuAddr = rand()};
    uint64_t xi64 = RemotePtrToI64(x);
    RemotePtrT y = RemotePtrFromI64(xi64);
    uint64_t yi64 = RemotePtrToI64(y);
    if (x.dpuId != y.dpuId || x.dpuAddr != y.dpuAddr || xi64 != yi64) {
        return false;
    }
    return true;
}

inline void RemotePtrPrint(RemotePtrT rPtr) {
    printf("(RemotePtrT){.dpuId = %x\t, .dpuAddr = %x}\n", rPtr.dpuId, rPtr.dpuAddr);
}

void MaxLinkAddrPrint(MaxLinkAddrT maxLinkAddr) {
    printf("MaxLinkAddrT.rPtr = ");
    RemotePtrPrint(maxLinkAddr.rPtr);
}

inline void TupleIdPrint(TupleIdT tupleId) {
    printf("(TupleIdT){.tableId = %d\t, .tupleAddr = %llx}\n", tupleId.tableId, tupleId.tupleAddr);
}

// MaxLink Related Function

inline HashAddrT* GetHashAddrsFromMaxLink(MaxLinkT* maxLink) {
    return (HashAddrT*)(maxLink->buffer + sizeof(TupleIdT) * maxLink->tupleIDCount);
}
inline HashAddrT* GetKthHashAddrFromMaxLink(MaxLinkT* maxLink, int i) {
    ArrayOverflowCheck(i >= 0 && i < maxLink->hashAddrCount);
    return GetHashAddrsFromMaxLink(maxLink) + i;
}

int GetMaxLinkSize(int tupleIdCount, int hashAddrCount) {
    return sizeof(MaxLinkT) + tupleIdCount * sizeof(TupleIdT) + hashAddrCount * sizeof(HashAddrT);
}

int BuildMaxLink(int tupleIdCount, int hashAddrCount, TupleIdT* tupleIds, HashAddrT* hashAddrs) {
    Unimplemented("BuildMaxLink");
    return 0;
}