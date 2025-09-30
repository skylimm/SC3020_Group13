#ifndef INDEXFILE_H
#define INDEXFILE_H
#include <stdint.h>
#include <stdio.h>
#include "bptree.h"

typedef struct
{
    FILE *fp;
    const char *path;

    // data_reads and data_writes is for I/O count
    uint64_t data_reads;
    uint64_t data_writes;
} IndexFile;

int index_open(IndexFile *fm, const char *path, const char *mode);
void index_close(IndexFile *fm);
int index_read_block(IndexFile *fm, uint32_t node_id, Node *out);
int index_write_block(IndexFile *fm, uint32_t node_id, const Node *in);
uint32_t index_alloc_block(IndexFile *fm, Node *zeroed);

#endif