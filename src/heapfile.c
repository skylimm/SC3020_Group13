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
    if (!hf || !csv_path) return -1;

    FILE* f = fopen(csv_path, "r");
    if (!f) {
        fprintf(stderr, "Cannot open CSV: %s\n", csv_path);
        return -1;
    }

    // prepare first empty block and append to file
    Block blk;
    memset(&blk, 0, sizeof(Block));
    block_set_used_count(&blk, 0);
    uint32_t cur_block_id = fm_alloc_block(&hf->fm, &blk);
    if (cur_block_id == (uint32_t)-1) {
        fclose(f);
        return -1;
    }
    hf->n_blocks = cur_block_id + 1;

    // read header line and build column index map
    char line[8192];
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return -1;
    }
    CsvIdx idx;
    if (parse_header_map(line, &idx) != 0) {
        fclose(f);
        fprintf(stderr, "Header does not contain required columns.\n");
        return -1;
    }

    const int cap = block_capacity_records(hf->schema.record_size);
    if (cap <= 0) {
        fclose(f);
        fprintf(stderr, "Record size too large for block.\n");
        return -1;
    }

    uint8_t recbuf[512];
    int slot = block_used_count(&blk); // should start at 0
    uint64_t accepted = 0, skipped = 0;

    // read each data row
    while (fgets(line, sizeof(line), f)) {
        Row r;
        if (parse_row_by_index(line, &idx, &r) != 0) {
            skipped++;
            continue; // ignore malformed lines
        }

        encode_row(&hf->schema, &r, recbuf);

        // if current block full, write it and start a new one
        if (slot >= cap) {
            if (fm_write_block(&hf->fm, cur_block_id, &blk) != 0) {
                fclose(f);
                return -1;
            }
            memset(&blk, 0, sizeof(Block));
            block_set_used_count(&blk, 0);
            cur_block_id = fm_alloc_block(&hf->fm, &blk);
            if (cur_block_id == (uint32_t)-1) {
                fclose(f);
                return -1;
            }
            hf->n_blocks = cur_block_id + 1;
            slot = 0;
        }

        // write record bytes into the in-memory block
        if (block_write_record(&blk, hf->schema.record_size, slot, recbuf) != 0) {
            fclose(f);
            fprintf(stderr, "Failed to write record into block (slot=%d)\n", slot);
            return -1;
        }
        slot++;
        block_set_used_count(&blk, (uint16_t)slot);
        accepted++;
    }

    // flush the last (possibly partially filled) block
    if (fm_write_block(&hf->fm, cur_block_id, &blk) != 0) {
        fclose(f);
        return -1;
    }

    fclose(f);

    // Optional: brief load summary to stderr (kept quiet on stdout)
    fprintf(stderr, "[load] accepted=%llu skipped=%llu blocks=%u rpb=%d rec_size=%u\n",
            (unsigned long long)accepted,
            (unsigned long long)skipped,
            hf->n_blocks,
            cap,
            hf->schema.record_size);

    return 0;
}

// scanning and printing first N records
int hf_scan_print_firstN(HeapFile* hf, int limit){
    if (!hf) return -1;
    if (limit < 0) limit = 0;  // 0 = print all

    uint8_t recbuf[512];
    Row r;
    int printed = 0;

    Block blk;
    for (uint32_t b = 0; b < hf->n_blocks; b++) {
        if (fm_read_block(&hf->fm, b, &blk) != 0)
            return -1;
        int used = block_used_count(&blk);
        int cap  = block_capacity_records(hf->schema.record_size);
        if (used > cap) used = cap; // safety

        for (int s = 0; s < used; s++) {
            if (block_read_record(&blk, hf->schema.record_size, s, recbuf) != 0)
                return -1;
            decode_row(&hf->schema, recbuf, &r);
            printf("%d,%s,%d,%d,%.3f,%d\n",
                   r.game_id, r.game_date,
                   r.home_team_id, r.visitor_team_id,
                   r.ft_pct_home, r.home_team_wins);
            printed++;
            if (limit > 0 && printed >= limit) return 0;
        }
    }
    return 0;
}
