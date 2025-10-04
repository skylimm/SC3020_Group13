#include "bptree.h"
#include "file_manager_btree.h"
#include <string.h>
#include <stdlib.h>

#define MAX_INT_CHILDREN (MAX_INTERNAL_KEYS + 1)

typedef struct
{
    float key;
    uint32_t node_id;
} ChildListEntry;

int pack_internals(ChildListEntry *child_list, int node_count, int level, int total_nodes, int *parent_count, ChildListEntry *parent_list, BtreeFileManager *fm);
int bulkload(float *lower_bound_array, int child_count, BtreeFileManager *fm);
