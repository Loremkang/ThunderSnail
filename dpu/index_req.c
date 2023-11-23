#include "index_req.h"
#include "shared_wram.h"

primary_index_dpu *IndexCheck(uint32_t indexId){
    return primary_index_dpu_check(indexId);
}

/*
    usage:
    __dma_aligned HashTableQueryReplyT reply_buffer;
    IndexGetOrInsertReq(pid, key, keyLen, tupleID, &reply_buffer);
*/
void IndexGetOrInsertReq(primary_index_dpu *pid, char *key, uint32_t keyLen, TupleIdT tupleId, HashTableQueryReplyT *reply) {
    __mram_ptr primary_index_entry *entry;
    metadata valMeta = {.type = TupleId, .id = tupleId.tableId};
    uint64_t val = tupleId.tupleAddr;

    entry = primary_index_dpu_get_or_insert(pid, key, keyLen, valMeta, val);
    if (entry) {
        __dma_aligned primary_index_entry entry_buffer;
        mram_read((__mram_ptr void *)entry, (void *)&entry_buffer, sizeof(primary_index_entry));
        if (entry_buffer.val_meta.type == TupleId) {
            reply->type = TupleId;
            reply->value.tupleId.tableId = entry_buffer.val_meta.id;
            reply->value.tupleId.tupleAddr = entry_buffer.val;
        } else if (entry_buffer.val_meta.type == MaxLinkAddr) {
            reply->type = MaxLinkAddr;
            reply->value.maxLinkAddr.rPtr = RemotePtrFromI64(entry_buffer.val);
        }
    } else {
        entry = primary_index_dpu_lookup(pid, key, keyLen);
        reply->type = HashAddr;
        reply->value.hashAddr.rPtr.dpuId = gDpuId;
        reply->value.hashAddr.rPtr.dpuAddr = (uint32_t)entry;
    }
}

void IndexUpdateReq(primary_index_dpu *pid, char *key, HashAddrT hashAddr, MaxLinkAddrT maxLinkAddr) {
    __mram_ptr primary_index_entry *entry = (__mram_ptr primary_index_entry *)hashAddr.rPtr.dpuAddr;
    metadata newValMeta = {.type = MaxLinkAddr};
    uint64_t newVal = RemotePtrToI64(maxLinkAddr.rPtr);
    primary_index_dpu_update_with_entry_addr(pid, entry, newValMeta, newVal);
}