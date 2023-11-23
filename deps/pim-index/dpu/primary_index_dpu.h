#ifndef __PRIMARY_INDEX_DPU_H
#define __PRIMARY_INDEX_DPU_H

#include <stdint.h>
#include <mram.h>
#include <stddef.h>
#include "mram_memory.h"
#include "../util/util.h"
#include "linear_mram_alloc.h"
#include "../util/param.h"

#define PRIMARY_INDEX_OK 0
#define PRIMARY_INDEX_ERR 1

#define PRIMARY_INDEX_BUCKETS_INITIAL_SIZE (16 * 1024)

typedef struct _metadata
{
    uint32_t type;
    uint32_t id;
} metadata;

typedef struct _primary_index_entry
{
    char key[16];
    uint32_t key_len;
    __mram_ptr struct _primary_index_entry *next;
    metadata val_meta;
    uint64_t val;
} primary_index_entry;

typedef struct _primary_index_dpu
{
    PRIMARY_INDEX_ID index_id;
    uint32_t buckets_size;
    uint32_t sizemask;
    uint32_t used;
    __mram_ptr primary_index_entry *buckets;
} primary_index_dpu;

void primary_index_dpu_init(primary_index_dpu *pid, __mram_ptr primary_index_entry *buckets_addr, PRIMARY_INDEX_ID index_id, uint32_t buckets_size);
void primary_index_dpu_init_buckets(primary_index_dpu *pid, uint32_t tasklet_id);
primary_index_dpu *primary_index_dpu_check(uint32_t index_id);
int primary_index_dpu_insert(primary_index_dpu *pid, char *key, uint32_t key_len, uint64_t val);
int primary_index_dpu_delete(primary_index_dpu *pid, char *key, uint32_t key_len);
__mram_ptr primary_index_entry *primary_index_dpu_lookup(const primary_index_dpu *pid, char *key, uint32_t key_len);
void primary_index_dpu_update_with_entry_addr(const primary_index_dpu *pid, __mram_ptr primary_index_entry *entry, metadata new_val_meta, uint64_t new_val);
__mram_ptr primary_index_entry *primary_index_dpu_get_or_insert(primary_index_dpu *pid, char *key, uint32_t key_len, metadata val_meta, uint64_t val);
void primary_index_dpu_test(primary_index_dpu *pid);

#endif