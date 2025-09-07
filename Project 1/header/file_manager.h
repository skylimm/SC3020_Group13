#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H
#include <stdint.h>
#include <stdio.h>
#include "block.h"

typedef struct {
    FILE* fp;
    const char* path;

    // data_reads and data_writes is for I/O count
    uint64_t data_reads;
    uint64_t data_writes;
} FileManager;

int  fm_open(FileManager* fm, const char* path, const char* mode); 
void fm_close(FileManager* fm);
int  fm_read_block(FileManager* fm, uint32_t block_id, Block* out);
int  fm_write_block(FileManager* fm, uint32_t block_id, const Block* in);
uint32_t fm_alloc_block(FileManager* fm, Block* zeroed); 

#endif
