#ifndef HEAPFILE_H
#define HEAPFILE_H
#include <stdint.h>
#include "schema.h"
#include "buffer_pool.h"

typedef struct {
    Schema      schema;
    FileManager fm;
    BufferPool  bp;
    uint32_t    n_blocks;   // data blocks
} HeapFile;

// for db
int  hf_create(HeapFile* hf, const char* path, const Schema* s, int buf_frames);
int  hf_open  (HeapFile* hf, const char* path, int buf_frames);
void hf_close (HeapFile* hf);

// // loading from txt
int  hf_load_csv(HeapFile* hf, const char* csv_path);

// stats and printing
uint32_t hf_count_records(HeapFile* hf);
int  hf_records_per_block(const HeapFile* hf);
void hf_print_stats(HeapFile* hf);
int  hf_scan_print_firstN(HeapFile* hf, int limit);

// minhwan: Record deletion functionality
int hf_delete_record(HeapFile* hf, uint32_t block_id, uint16_t slot_id);

#endif
