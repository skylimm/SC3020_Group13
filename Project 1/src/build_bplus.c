#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bptree.h"
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

    // sort the entries by key, 
    if (count > 1)
        qsort(entries, count, sizeof(KeyPointer), compare_key_pointer);

    // open B+tree file (one page per node)
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
    uint32_t leaf_node_ids[1000];
    leaf_node_ids[0] = node_id;


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
        
        link_leaf_node(curr, new_id);

        if (btfm_write_node(&fm, curr) != 0) {
            fprintf(stderr, "Error writing node %u to disk\n", curr->node_id);
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
        leaf_node_ids[leaf_count - 1] = new_id;

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

    link_leaf_node(curr, UINT32_MAX);
    
    // Persist the final (partially filled) leaf
    if (btfm_write_node(&fm, curr) != 0) {
        fprintf(stderr, "Error writing final node %u to disk\n", curr->node_id);
        free(curr);
        btfm_close(&fm);
        free(entries);
        return -1;
    }

    // build the upper levels using bulkloading method
    int total_nodes = 0;
    if (bulkload(array, leaf_count, &fm, &total_nodes)== -1) {
        fprintf(stderr, "bulkload failed\n");
        free(curr);
        btfm_close(&fm);
        free(entries);
        return -1;
    }

    printf("Parameters n : %d\n", MAX_LEAF_KEYS + 1);
    printf("Total leaf nodes: %d\n", leaf_count);
    printf("Total nodes (incl. root): %d\n", total_nodes);
    printf("Number of levels: 2\n");
    
    
    free(curr);
    btfm_close(&fm);
    free(entries);
    return 0;
}

int bulkload(float *lower_bound_array, int child_count, BtreeFileManager *fm, int *total_nodes)
{

    if (child_count == 1)
        return 0; // single leaf node as root
    else
    {
        ChildListEntry child_list[100];
        for (int i = 0; i < child_count; i++)
        {
            child_list[i].key = lower_bound_array[i];
            child_list[i].node_id = i;
        }
        int level = 1;
        *total_nodes = child_count;

        while (1)
        {
            level += 1;
            int parent_count = 0;
            ChildListEntry parent_list[100];

            if (pack_internals(child_list, child_count, level, *total_nodes, &parent_count, parent_list, fm) == -1)
            {
                printf("Error in packing internal nodes\n");
                return -1;
            }
            if (parent_count == 1)
            {
                *total_nodes += parent_count;
                return 0;
            }
            else
            {
                // prepare for next iteration
                *total_nodes += parent_count;
                child_count = parent_count;
                memcpy(child_list, parent_list, parent_count * sizeof(ChildListEntry));
            }
        }
    }

    return 0;
}

int pack_internals(ChildListEntry *child_list, int node_count, int level, int total_nodes, int *parent_count, ChildListEntry *parent_list, BtreeFileManager *fm)
{
    if (node_count < MAX_INT_CHILDREN)
    {
        printf("Filling all into one node\n");
        // fill it all into one node
        // create node
        Node *n = malloc(sizeof(Node));
        node_init(n, level, total_nodes); 

        // fill node
        for (int i = 1; i < node_count; i++)
        {
            node_write_node_key(n, child_list[i].key, child_list[i].node_id);
            // also print content of root node nicely. prev pointer and all keys with their node ids
            printf("Root node key %d: %.2f\n", i, child_list[i].key);
        }
        if (btfm_write_node(fm, n) != 0)
        {
            fprintf(stderr, "Error writing root node to disk\n");
            free(n);
            btfm_close(fm);
            return -1;
        }
        free(n);
        *parent_count += 1;
    }
    else
    {
        // Divide the child nodes into groups of MAX_INT_CHILDREN, each group gets one parent node
        if (node_count % MAX_INT_CHILDREN == 0)
        {
            printf("Splitting at every %d nodes\n", node_count / MAX_INT_CHILDREN);

            int num_nodes = node_count / MAX_INT_CHILDREN;

            for (int i = 0; i < num_nodes; i++)
            {

                // create_node
                Node *n = malloc(sizeof(Node));
                node_init(n, level, total_nodes + *parent_count); // temp id=0
                // put the first value into parentlist
                parent_list[*parent_count].key = child_list[i * MAX_INT_CHILDREN].key;
                parent_list[*parent_count].node_id = child_list[i * MAX_INT_CHILDREN].node_id;

                *parent_count += 1;

                // fill node
                for (int j = i * MAX_INT_CHILDREN + 1; j < (i + 1) * MAX_INT_CHILDREN; j++)
                {
                    node_write_node_key(n, child_list[j].key, child_list[j].node_id);
                    // also print content of root node nicely. prev pointer and all keys with their node ids
                    printf("Root node key %d: %.2f\n", j, child_list[j].key);
                }

                // write the node to disk
                if (btfm_write_node(fm, n) != 0)
                {
                    fprintf(stderr, "Error writing root node to disk\n");
                    free(n);
                    btfm_close(fm);
                    return -1;
                }
                free(n);
            }
        }

        else if ((node_count % MAX_INT_CHILDREN) - 1 < MIN_INTERNAL_KEYS && node_count % MAX_INT_CHILDREN > 0)
        {
            printf("Splitting with borrowing for the last two nodes\n");
            // split is every num_nodes multiple except for the last two nodes
            // settle the split for the last two nodes, borrow from the second last node
            int num_nodes = node_count / MAX_INT_CHILDREN + 1;
            int num_borrow = MIN_INTERNAL_KEYS - (node_count % MAX_INT_CHILDREN) + 1;
            int borrow_from = ((num_nodes - 1) * MAX_INT_CHILDREN) - num_borrow;

            printf("num_nodes: %d, num_borrow: %d, borrow_from: %d\n", num_nodes, num_borrow, borrow_from);

            for (int i = 0; i < num_nodes - 2; i++)
            {
                printf("Creating node %d\n", i);
                // create_node
                Node *n = malloc(sizeof(Node));
                node_init(n, level, total_nodes + *parent_count); // temp id=0
                // put the first value into parentlist
                parent_list[*parent_count].key = child_list[i * MAX_INT_CHILDREN].key;
                parent_list[*parent_count].node_id = child_list[i * MAX_INT_CHILDREN].node_id;

                *parent_count += 1;

                // fill node
                for (int j = i * MAX_INT_CHILDREN + 1; j < (i + 1) * MAX_INT_CHILDREN; j++)
                {
                    node_write_node_key(n, child_list[j].key, child_list[j].node_id);
                    // also print content of root node nicely. prev pointer and all keys with their node ids
                    printf("Root node key %d: %.2f\n", j, child_list[j].key);
                }
                printf("\n");

                // write the node to disk
                if (btfm_write_node(fm, n) != 0)
                {
                    fprintf(stderr, "Error writing root node to disk\n");
                    free(n);
                    btfm_close(fm);
                    return -1;
                }
                free(n);
            }

            // handle the last two nodes

            // create node
            Node *n = malloc(sizeof(Node));
            node_init(n, level, total_nodes + *parent_count);

            // put the first value into parentlist
            parent_list[*parent_count].key = child_list[(num_nodes - 2) * MAX_INT_CHILDREN].key;
            parent_list[*parent_count].node_id = child_list[(num_nodes - 2) * MAX_INT_CHILDREN].node_id;

            *parent_count += 1;

            printf("Creating node %d\n", num_nodes - 2);

            for (int j = (num_nodes - 2) * MAX_INT_CHILDREN + 1; j < borrow_from; j++)
            {
                node_write_node_key(n, child_list[j].key, child_list[j].node_id);
                // also print content of root node nicely. prev pointer and all keys with their node ids
                printf("Root node key %d: %.2f\n", j, child_list[j].key);
            }

            // write the node to disk
            if (btfm_write_node(fm, n) != 0)
            {
                fprintf(stderr, "Error writing root node to disk\n");
                free(n);
                btfm_close(fm);
                return -1;
            }
            free(n);

            // create node
            n = malloc(sizeof(Node));
            node_init(n, level, total_nodes + *parent_count);

            // put the first value into parentlist
            parent_list[*parent_count].key = child_list[borrow_from].key;
            parent_list[*parent_count].node_id = child_list[borrow_from].node_id;

            *parent_count += 1;

            printf("Creating node %d\n", num_nodes - 1);
            // fill node
            for (int j = borrow_from + 1; j < node_count; j++)
            {
                node_write_node_key(n, child_list[j].key, child_list[j].node_id);
                // also print content of root node nicely. prev pointer and all keys with their node ids
                printf("Root node key %d: %.2f\n", j, child_list[j].key);
            }

            // write the node to disk
            if (btfm_write_node(fm, n) != 0)
            {
                fprintf(stderr, "Error writing root node to disk\n");
                free(n);
                btfm_close(fm);
                return -1;
            }
            free(n);
        }
    }
    return 0;
}