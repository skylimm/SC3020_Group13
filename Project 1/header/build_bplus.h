#ifndef BUILD_BPLUS_H
#define BUILD_BPLUS_H

#include "bptree.h"
#include "heapfile.h"
#include "file_manager_btree.h"

typedef struct
{
    float key;
    uint32_t node_id;
} ChildListEntry;

int scan_db(HeapFile *hf);
int bulkload(float *lower_bound_array, int child_count, BtreeFileManager *fm, int *total_nodes);
int pack_internals(ChildListEntry *child_list, int node_count, int level, int total_nodes, int *parent_count, ChildListEntry *parent_list, BtreeFileManager *fm);

#endif // BUILD_BPLUS_H