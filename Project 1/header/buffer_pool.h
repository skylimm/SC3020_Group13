#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H
#include <stdint.h>
#include <stdbool.h>
#include "block.h"
#include "file_manager.h"

typedef struct {
    bool     valid;
    bool     dirty;
    uint32_t block_id;
    Block    block;
    uint64_t tick; // last access time
} Frame;

typedef struct {
    FileManager* fm;
    Frame*   frames;
    int      capacity;
    uint64_t clock_tick;
} BufferPool;

int  bp_init(BufferPool* bp, FileManager* fm, int capacity);
void bp_destroy(BufferPool* bp);
Block* bp_fetch(BufferPool* bp, uint32_t block_id); 
void   bp_mark_dirty(BufferPool* bp, uint32_t block_id);
int    bp_flush_all(BufferPool* bp);

#endif
