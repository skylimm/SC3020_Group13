#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bptree.h"
#include "build_bplus.h"
#include "file_manager_btree.h"

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

    // open B+tree file (one page per node)
    // minhwan: Delete old file first to ensure we start fresh
    remove("btree.db");
    
    BtreeFileManager fm;
    if (btfm_open(&fm, "btree.db", NODE_SIZE) != 0) {
        fprintf(stderr, "Could not open btree.db\n");
        free(entries);
        return -1;
    }

    // nothing to index?
    if (count == 0) {
        btfm_close(&fm);
        free(entries);
        return 0;
    }

    // allocate first leaf id
    uint32_t node_id;
    if (btfm_alloc_node(&fm, &node_id) != 0) {
        fprintf(stderr, "alloc node failed\n");
        btfm_close(&fm);
        free(entries);
        return -1;
    }
    // build leaves and persist as they fill
    int leaf_count = 0;
    Node *curr = malloc(sizeof(Node));
    if (!curr) {
        btfm_close(&fm);
        free(entries);
        return -1;
    }
    node_init(curr, 1, node_id);
    leaf_count = 1;

    float array[1000];
    uint32_t leaf_node_ids[1000]; // minhwan: Store actual allocated node IDs
    leaf_node_ids[0] = node_id; // minhwan: Store first leaf node ID


    for (size_t i = 0; i < count; i++) {
        int wr = node_write_record_key(curr,
                                    entries[i].key,
                                    entries[i].block_id,
                                    entries[i].slot_id);
        if (wr >= 0) continue;
        array[leaf_count - 1] = curr->lower_bound;
        uint32_t new_id;
        if (btfm_alloc_node(&fm, &new_id) != 0) {
            fprintf(stderr, "alloc node failed\n");
            free(curr);
            btfm_close(&fm);
            free(entries);
            return -1;
        }
        Node *next = malloc(sizeof(Node));
        if (!next) {
            free(curr);
            btfm_close(&fm);
            free(entries);
            return -1;
        }
        node_init(next, 1, new_id);
        leaf_count++;
        leaf_node_ids[leaf_count - 1] = new_id; // minhwan: Store actual leaf node ID

        link_leaf_node(curr, new_id);

        if (btfm_write_node(&fm, curr) != 0) {
            fprintf(stderr, "Error writing node %u to disk\n", curr->node_id);
            free(next);
            free(curr);
            btfm_close(&fm);
            free(entries);
            return -1;
        }

        // Move to the new leaf and retry the same entry
        free(curr);
        curr = next;

        if (node_write_record_key(curr,
                                entries[i].key,
                                entries[i].block_id,
                                entries[i].slot_id) < 0) {
            fprintf(stderr, "Unexpected: write failed on fresh leaf\n");
            free(curr);
            btfm_close(&fm);
            free(entries);
            return -1;
        }
    }
    array[leaf_count - 1] = curr->lower_bound;
    // Persist the final (partially filled) leaf
    if (btfm_write_node(&fm, curr) != 0) {
        fprintf(stderr, "Error writing final node %u to disk\n", curr->node_id);
        free(curr);
        btfm_close(&fm);
        free(entries);
        return -1;
    }

    printf("Parameters n : %d\n", MAX_LEAF_KEYS + 1);
    printf("Total nodes (incl. root): %d\n", leaf_count + 1); // minhwan: Don't increment leaf_count, just add 1 for display
    printf("Number of levels: 2\n");
    
    // minhwan: Properly allocate root node ID instead of using leaf_count
    uint32_t root_id;
    if (btfm_alloc_node(&fm, &root_id) != 0) {
        fprintf(stderr, "Failed to allocate root node\n");
        btfm_close(&fm);
        free(entries);
        return -1;
    }
    
    Node *root = malloc(sizeof(Node));
    node_init(root, 2, root_id); // minhwan: Use proper allocated root_id

    set_int_node_lb(root, array[0]);
    for (int i = 0; i < leaf_count; i++) { // minhwan: Include ALL leaf nodes (0 to leaf_count-1)
        node_write_node_key(root, array[i], leaf_node_ids[i]); // minhwan: Use actual stored node ID
        //also print content of root node nicely. prev pointer and all keys with their node ids
        printf("Root node key %d: %.2f\n", i, array[i]);
    }
    if (btfm_write_node(&fm, root) != 0) {
        fprintf(stderr, "Error writing root node to disk\n");
        free(root);
        free(curr);
        btfm_close(&fm);
        free(entries);
        return -1;
    }
    free(root);
    
    free(curr);
    btfm_close(&fm);
    free(entries);
    return 0;
}
