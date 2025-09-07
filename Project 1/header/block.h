#ifndef BLOCK_H
#define BLOCK_H
#include <stdint.h>

#define BLOCK_SIZE 4096
#define BLOCK_HDR_SIZE 4  

typedef struct {
    uint8_t bytes[BLOCK_SIZE];
} Block;

static inline uint16_t block_used_count(const Block* b) {
    return (uint16_t)(b->bytes[0] | (b->bytes[1] << 8));
}
static inline void block_set_used_count(Block* b, uint16_t v) {
    b->bytes[0] = (uint8_t)(v & 0xFF);
    b->bytes[1] = (uint8_t)((v >> 8) & 0xFF);
}

int  block_capacity_records(uint16_t record_size);
int  block_write_record(Block* b, uint16_t record_size, int slot, const uint8_t* rec);
int  block_read_record (const Block* b, uint16_t record_size, int slot, uint8_t* out);

#endif
