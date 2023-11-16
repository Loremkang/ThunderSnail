#include "common_base_struct.h"
#include <stdlib.h>

// RemotePtr Related Function

// internally used only
typedef union {
    RemotePtrT rPtr;
    uint64_t i64;
} RemotePtrConvertT;

inline uint64_t RemotePtrToI64(RemotePtrT rPtr) {
    RemotePtrConvertT tmp;
    tmp.rPtr = rPtr;
    return tmp.i64;
}

inline RemotePtrT RemotePtrFromI64(uint64_t i64) {
    RemotePtrConvertT tmp;
    tmp.i64 = i64;
    return tmp.rPtr;
}

inline bool RemotePtrInvalid(RemotePtrT rPtr) {
    return RemotePtrToI64(rPtr) == RemotePtrToI64(INVALID_REMOTEPTR);
}

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

bool TupleIdOrMaxLinkAddrEqual(TupleIdOrMaxLinkAddrT a, TupleIdOrMaxLinkAddrT b) {
    if (a.type != b.type) {
        return false;
    }
    if (a.type == TupleId) {
        return a.value.tupleId.tableId == b.value.tupleId.tableId &&
                a.value.tupleId.tupleAddr == b.value.tupleId.tupleAddr;
    } else if (a.type == MaxLinkAddr) {
        return RemotePtrToI64(a.value.maxLinkAddr.rPtr) == RemotePtrToI64(b.value.maxLinkAddr.rPtr);
    } else {
        ValueOverflowCheck(0);
    }
}


// MaxLink Related Function

// private
static inline TupleIdT* GetTupleIDsFromMaxLink(MaxLink* maxLink) {
    return (TupleIdT*)(maxLink->buffer);
}

// Cannot pass reference. Passing pointer instead.
inline TupleIdT* GetKthTupleIDFromMaxLink(MaxLink* maxLink, int i) {
    ArrayOverflowCheck(i >= 0 && i < maxLink->tupleIDCount);
    return GetTupleIDsFromMaxLink(maxLink) + i;
}

// private
static inline HashAddrT* GetHashAddrsFromMaxLink(MaxLink* maxLink) {
    return (HashAddrT*)(maxLink->buffer + sizeof(TupleIdT) * maxLink->tupleIDCount);
}

inline HashAddrT* GetKthHashAddrFromMaxLink(MaxLink* maxLink, int i) {
    ArrayOverflowCheck(i >= 0 && i < maxLink->hashAddrCount);
    return GetHashAddrsFromMaxLink(maxLink) + i;
}

int GetMaxLinkSize(int tupleIdCount, int hashAddrCount) {
    return sizeof(MaxLink) + tupleIdCount * sizeof(TupleIdT) + hashAddrCount * sizeof(HashAddrT);
}

int BuildMaxLink(int tupleIdCount, int hashAddrCount, TupleIdT* tupleIds, HashAddrT* hashAddrs) {
    Unimplemented("BuildMaxLink");
}