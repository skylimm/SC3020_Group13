#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bptree.h"
#include "heapfile.h"
#include "build_bplus.h"

typedef struct {
    float    key;
    uint32_t block_id;
    uint16_t slot_id;
} KeyPointer;

static int ensure_capacity(KeyPointer **entries, size_t *capacity, size_t needed)
{
    if (needed <= *capacity)
        return 0;
    size_t new_cap = (*capacity == 0) ? 256 : *capacity;
    while (new_cap < needed)
        new_cap *= 2;
    KeyPointer *tmp = realloc(*entries, new_cap * sizeof(KeyPointer));
    if (!tmp)
        return -1;
    *entries = tmp;
    *capacity = new_cap;
    return 0;
}

static int compare_key_pointer(const void *a, const void *b)
{
    const KeyPointer *ka = (const KeyPointer *)a;
    const KeyPointer *kb = (const KeyPointer *)b;
    if (ka->key < kb->key)
        return -1;
    if (ka->key > kb->key)
        return 1;
    if (ka->block_id < kb->block_id)
        return -1;
    if (ka->block_id > kb->block_id)
        return 1;
    if (ka->slot_id < kb->slot_id)
        return -1;
    if (ka->slot_id > kb->slot_id)
        return 1;
    return 0;
}

int scan_db(HeapFile *hf)
{
    if (!hf)
        return -1;

    uint8_t recbuf[512];
    Row r;

    KeyPointer *entries = NULL;
    size_t count = 0;
    size_t capacity = 0;

    for (uint32_t b = 0; b < hf->n_blocks; b++)
    {
        Block *cur = bp_fetch(&hf->bp, b);
        if (!cur)
        {
            free(entries);
            return -1;
        }

        int used = block_used_count(cur);
        int cap = block_capacity_records(hf->schema.record_size);
        if (used > cap)
            used = cap;

        for (int s = 0; s < used; s++)
        {
            if (block_read_record(cur, hf->schema.record_size, s, recbuf) != 0)
            {
                free(entries);
                return -1;
            }
            decode_row(&hf->schema, recbuf, &r);

            if (ensure_capacity(&entries, &capacity, count + 1) != 0)
            {
                free(entries);
                return -1;
            }

            entries[count].key = r.ft_pct_home;
            entries[count].block_id = b;
            entries[count].slot_id = (uint16_t)s;
            count++;
        }
    }

    if (count > 1)
        qsort(entries, count, sizeof(KeyPointer), compare_key_pointer);
    int node_count = 1;
    Node *node = malloc(sizeof(Node));
    Node *curr = node;
    node_init(node, 1, node_count++);
    for (size_t i = 0; i < count; i++){
        if (node_write_record_key(curr, entries[i].key, entries[i].block_id, entries[i].slot_id) < 0){
            Node *new_node = malloc(sizeof(Node));
            node_init(new_node, 1, node_count++);
            link_leaf_node(curr, node_count);
            curr = new_node;
            if (node_write_record_key(curr, entries[i].key, entries[i].block_id, entries[i].slot_id) < 0){
                printf("Error writing record key to new node\n");
            }
        }
    }
    printf("Total leaf nodes: %d\n", node_count - 1);
    printf("Total entries: %zu\n", count);
    free(entries);
    return 0;
}
