#include "heapfile.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// create new database file
int hf_create(HeapFile* hf, const char* path, const Schema* s, int buf_frames){
    if (!hf || !path || !s) return -1;
    hf->schema = *s;                       // copy schema (value semantics)
    if (fm_open(&hf->fm, path, "wb+"))     // create/truncate binary db file
        return -1;
    if (bp_init(&hf->bp, &hf->fm, buf_frames))
        return -1;
    hf->n_blocks = 0;
    return 0;
}

int hf_open(HeapFile* hf, const char* path, int buf_frames){
    if (!hf || !path) return -1;
    if (fm_open(&hf->fm, path, "rb+"))     // open existing
        return -1;
    if (bp_init(&hf->bp, &hf->fm, buf_frames))
        return -1;

    // derive n_blocks from file size
    if (fseek(hf->fm.fp, 0, SEEK_END)!=0) return -1;
    long sz = ftell(hf->fm.fp);
    if (sz < 0) return -1;
    hf->n_blocks = (uint32_t)(sz / BLOCK_SIZE);

    // reconstruct schema (simple approach for Part 1)
    schema_init_default(&hf->schema);
    return 0;
}

void hf_close(HeapFile* hf){
    if (!hf) return;
    bp_flush_all(&hf->bp);
    bp_destroy(&hf->bp);
    fm_close(&hf->fm);
}

// helper functions for stats
int hf_records_per_block(const HeapFile* hf){
    if (!hf) return 0;
    return block_capacity_records(hf->schema.record_size);
}

uint32_t hf_count_records(HeapFile* hf){
    if (!hf) return 0;
    uint32_t total = 0;
    Block blk;
    for (uint32_t b = 0; b < hf->n_blocks; b++) {
        if (fm_read_block(&hf->fm, b, &blk) != 0) break;
        total += block_used_count(&blk);
    }
    return total;
}

void hf_print_stats(HeapFile* hf){
    if (!hf) return;
    schema_print(&hf->schema);
    int rpb = hf_records_per_block(hf);
    uint32_t nrecs = hf_count_records(hf);
    printf("Block size: %d\n", BLOCK_SIZE);
    printf("Records per block: %d\n", rpb);
    printf("#Blocks: %u (file size ~ %u bytes)\n", hf->n_blocks, hf->n_blocks * BLOCK_SIZE);
    printf("#Records: %u\n", nrecs);
    printf("I/O counts: reads=%llu writes=%llu\n",
           (unsigned long long)hf->fm.data_reads,
           (unsigned long long)hf->fm.data_writes);
}


// this part parse th csv rows, encode into records and store into blocks
int hf_load_csv(HeapFile* hf, const char* csv_path){
    FILE* f = fopen(csv_path, "r");
    if (!f) return -1;

    char line[8192];
    // read header
    if (!fgets(line, sizeof(line), f)) { fclose(f); return -1; }

    CsvIdx idx;
    if (parse_header_map(line, &idx) != 0) {
        fclose(f);
        fprintf(stderr, "Header does not contain required columns.\n");
        return -1;
    }

    // // allocate first block on disk
    // Block zero;
    // memset(&zero, 0, sizeof(Block));
    // uint32_t cur_block_id = fm_alloc_block(&hf->fm, &zero);
    // hf->n_blocks = cur_block_id + 1;

    // // fetch block from buffer pool
    // Block* cur = bp_fetch(&hf->bp, cur_block_id);
    // block_set_used_count(cur, 0);
    // bp_mark_dirty(&hf->bp, cur_block_id);

    // allocate block on disk
    Block zero;
    memset(&zero, 0, sizeof(Block));
    uint32_t cur_block_id = fm_alloc_block(&hf->fm, &zero);
    hf->n_blocks = cur_block_id + 1;

    // manually put it into the buffer pool
    Block* cur = bp_fetch(&hf->bp, cur_block_id);
    memset(cur, 0, sizeof(Block));
    block_set_used_count(cur, 0);
    bp_mark_dirty(&hf->bp, cur_block_id);

    const int cap = block_capacity_records(hf->schema.record_size);
    uint8_t recbuf[512];
    int slot = 0;
    Row r;

    while (fgets(line, sizeof(line), f)) {
        if (parse_row_by_index(line, &idx, &r) != 0) continue;
        encode_row(&hf->schema, &r, recbuf);

        if (slot >= cap) {
            // allocate a new block when current is full
            Block z; memset(&z, 0, sizeof(Block));
            cur_block_id = fm_alloc_block(&hf->fm, &z);
            hf->n_blocks = cur_block_id + 1;

            cur = bp_fetch(&hf->bp, cur_block_id);
            block_set_used_count(cur, 0);
            bp_mark_dirty(&hf->bp, cur_block_id);
            slot = 0;
        }

        block_write_record(cur, hf->schema.record_size, slot, recbuf);
        slot++;
        block_set_used_count(cur, (uint16_t)slot);
        bp_mark_dirty(&hf->bp, cur_block_id);
    }

    // flush dirty blocks to disk
    bp_flush_all(&hf->bp);
    fclose(f);
    return 0;
}

int hf_scan_print_firstN(HeapFile* hf, int limit){
    uint8_t recbuf[512];
    Row r;
    int printed = 0;

    for (uint32_t b = 0; b < hf->n_blocks; b++) {
        // fetch block via buffer pool
        Block* cur = bp_fetch(&hf->bp, b);
        if (!cur) return -1;

        int used = block_used_count(cur);
        int cap  = block_capacity_records(hf->schema.record_size);
        if (used > cap) used = cap;

        for (int s = 0; s < used; s++) {
            block_read_record(cur, hf->schema.record_size, s, recbuf);
            decode_row(&hf->schema, recbuf, &r);

            printf("%d,%s,%d,%d,%.3f,%d\n",
                   r.game_id, r.game_date,
                   r.home_team_id, r.visitor_team_id,
                   r.ft_pct_home, r.home_team_wins);

            if (limit > 0 && ++printed >= limit) return 0;
        }
    }
    return 0;
}

// minhwan: Delete a specific record from the heap file
int hf_delete_record(HeapFile* hf, uint32_t block_id, uint16_t slot_id)
{
    if (!hf || block_id >= hf->n_blocks) return -1;
    
    // Fetch the block from buffer pool
    Block* cur = bp_fetch(&hf->bp, block_id);
    if (!cur) return -1;
    
    int used = block_used_count(cur);
    if (slot_id >= used) return -1; // Invalid slot
    
    // Compact the block by moving records after the deleted slot forward
    uint8_t temp_buf[512];
    for (int s = slot_id + 1; s < used; s++) {
        // Read record from slot s
        if (block_read_record(cur, hf->schema.record_size, s, temp_buf) == 0) {
            // Write it to slot s-1
            block_write_record(cur, hf->schema.record_size, s - 1, temp_buf);
        }
    }
    
    // Update used count
    block_set_used_count(cur, used - 1);
    bp_mark_dirty(&hf->bp, block_id);
    
    return 0;
}
