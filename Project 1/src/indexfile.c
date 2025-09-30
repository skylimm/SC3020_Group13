#include "file_manager.h"
#include <string.h>

int index_open(IndexFile *fm, const char *path, const char *mode);
void index_close(IndexFile *fm);
int index_read_block(IndexFile *fm, uint32_t node_id, Node *out);
int index_write_block(IndexFile *fm, uint32_t node_id, const Node *in);
uint32_t index_alloc_block(IndexFile *fm, Node *zeroed);
