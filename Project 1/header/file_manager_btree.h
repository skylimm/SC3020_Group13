#ifndef FILE_MANAGER_BTREE_H
#define FILE_MANAGER_BTREE_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include "bptree.h"   // Node, NODE_SIZE, encode_node, decode_node

#ifdef __cplusplus
extern "C" {
#endif

typedef struct BtreeFileManager {
    FILE   *fp;
    size_t  page_size;   // must equal NODE_SIZE
} BtreeFileManager;

// Open (create if missing). page_size must be NODE_SIZE.
int  btfm_open(BtreeFileManager *fm, const char *path, size_t page_size);

// Flush buffers and close file.
int  btfm_close(BtreeFileManager *fm);

// fsync/flush buffers.
int  btfm_sync(BtreeFileManager *fm);

// Allocate a new page for a node at EOF and return its node_id.
int  btfm_alloc_node(BtreeFileManager *fm, uint32_t *out_node_id);

// Write (persist) node n at offset n->node_id * NODE_SIZE.
int  btfm_write_node(BtreeFileManager *fm, const Node *n);

// Read node by node_id into *out.
int  btfm_read_node(BtreeFileManager *fm, uint32_t node_id, Node *out);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // FILE_MANAGER_BTREE_H