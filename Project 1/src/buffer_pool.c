#include "buffer_pool.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>

// this file is for buffer pool manager with LRU replacement
// each block frame tracks its block_id, dirty bit, and last access tick
// if the pool is full and a new block is fetched, the lru block is evicted
// if the evicted block is marked dirty, it is first written back to disk
int bp_init(BufferPool *bp, FileManager *fm, int capacity)
{
    bp->fm = fm;
    bp->capacity = capacity;
    bp->clock_tick = 0;
    bp->frames = (Frame *)calloc(capacity, sizeof(Frame));
    return bp->frames ? 0 : -1;
}
void bp_destroy(BufferPool *bp)
{
    if (!bp->frames)
        return;
    bp_flush_all(bp);
    free(bp->frames);
    bp->frames = NULL;
}

static int find_frame(BufferPool *bp, uint32_t block_id)
{
    for (int i = 0; i < bp->capacity; i++)
    {
        if (bp->frames[i].valid && bp->frames[i].block_id == block_id)
            return i;
    }
    return -1;
}
static int pick_victim(BufferPool *bp)
{
    // choose invalid first; else LRU (smallest tick)
    int victim = -1;
    uint64_t best = ULLONG_MAX;
    for (int i = 0; i < bp->capacity; i++)
    {
        if (!bp->frames[i].valid)
            return i;
        if (bp->frames[i].tick < best)
        {
            best = bp->frames[i].tick;
            victim = i;
        }
    }
    return victim;
}

Block *bp_fetch(BufferPool *bp, uint32_t block_id)
{
    bp->clock_tick++;
    int idx = find_frame(bp, block_id);
    if (idx >= 0)
    {
        bp->frames[idx].tick = bp->clock_tick;
        return &bp->frames[idx].block;
    }
    // miss → evict victim
    int v = pick_victim(bp);
    if (v < 0)
        return NULL;
    // flush if dirty
    if (bp->frames[v].valid && bp->frames[v].dirty)
    {
        fm_write_block(bp->fm, bp->frames[v].block_id, &bp->frames[v].block);
        bp->frames[v].dirty = false;
    }
    // load new block
    if (fm_read_block(bp->fm, block_id, &bp->frames[v].block) != 0)
        return NULL;
    bp->frames[v].valid = true;
    bp->frames[v].dirty = false;
    bp->frames[v].block_id = block_id;
    bp->frames[v].tick = bp->clock_tick;
    return &bp->frames[v].block;
}

void bp_mark_dirty(BufferPool *bp, uint32_t block_id)
{
    int idx = find_frame(bp, block_id);
    if (idx >= 0)
        bp->frames[idx].dirty = true;
}

int bp_flush_all(BufferPool *bp)
{
    int err = 0;
    for (int i = 0; i < bp->capacity; i++)
    {
        if (bp->frames[i].valid && bp->frames[i].dirty)
        {
            if (fm_write_block(bp->fm, bp->frames[i].block_id, &bp->frames[i].block) != 0)
                err = -1;
            bp->frames[i].dirty = false;
        }
    }
    return err;
}
