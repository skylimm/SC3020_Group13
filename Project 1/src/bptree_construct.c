#include "bptree_construct.h"
#include <string.h>
#include <stdlib.h>


int bulkload(float *lower_bound_array, int child_count, BtreeFileManager *fm)
{

    if (child_count == 1)
        return 0 ; // single leaf node as root
    else {
        ChildListEntry child_list[100];
        for (int i = 0; i < child_count; i++)
        {
            child_list[i].key = lower_bound_array[i];
            child_list[i].node_id = i; 
        }
        int level = 1;
        int total_nodes = child_count;


        while (1) {
            int parent_count = 0;
            ChildListEntry parent_list[100];

            pack_internals(child_list, child_count, level, total_nodes, &parent_count, parent_list, fm);
            if (parent_count == 1) {
                return 0;
            } else {
                // prepare for next iteration
                total_nodes += parent_count;
                child_count = parent_count;
                level += 1;
                memcpy(child_list, parent_list, parent_count*sizeof(ChildListEntry));
            }
        }
    }

    return 0;
}



int pack_internals(ChildListEntry *child_list, int node_count, int level, int total_nodes, int *parent_count, ChildListEntry *parent_list, BtreeFileManager *fm)
{
    printf("MAX_INT_CHILDREN: %d\n", MAX_INT_CHILDREN);
    printf("node_count: %d\n", node_count);
    printf("level: %d\n", level);
    printf("child_list:\n");

    for (int i = 0; i < node_count; i++) {
        printf("  [%d] key: %.2f, node_id: %u\n", i, child_list[i].key, child_list[i].node_id);
    }

    if (node_count < MAX_INT_CHILDREN)
    {
        printf("Filling all into one node\n");
        // fill it all into one node
        // create_node();
        Node *n = malloc(sizeof(Node));
        node_init(n, level, total_nodes); // temp id=0
        // fill_node();
        for (int i = 1; i < node_count - 1; i++)
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
        *parent_count+=1;
    }
    else
    {
        // make the divisions
        // Replace 'child_nodes' with 'node_count' and declare needed variables
        if (node_count % MAX_INT_CHILDREN == 0)
        {
            printf("Splitting at every %d nodes\n", node_count / MAX_INT_CHILDREN);

            int num_nodes = node_count / MAX_INT_CHILDREN;

            for (int i = 0; i < num_nodes; i++) {
                
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

// alt main function just to test the bulk loading
// int main(void)
// {
//     float test_keys[] = {0.12f, 0.13f, 0.45f, 0.78f, 1.01f, 1.23f, 1.57f, 2.34f, 3.21f, 4.56f, 5.67f, 6.78f};
//     int child_count = (int)(sizeof(test_keys) / sizeof(test_keys[0]));

//     BtreeFileManager fm;
//     if (btfm_open(&fm, "btree_test.db", NODE_SIZE) != 0)
//     {
//         fprintf(stderr, "Failed to open btree_test.db\n");
//         return 1;
//     }

//     int rc = bulkload(test_keys, child_count, &fm);
//     if (rc != 0)
//         fprintf(stderr, "bulkload failed with code %d\n", rc);
//     else
//         printf("bulkload completed for %d keys\n", child_count);

//     btfm_close(&fm);
//     return rc == 0 ? 0 : 1;
// }

//PSUEDOCODE
/*
    FUNCTION BULKLOAD(data):

        create all the leaf nodes
        the array of min = create_leaf_nodes(data)
        if num_leaf == 1:
            return the single leaf node as root
        else:

            cur_level ← leaves        # current level nodes (children for the next level)
            level = 0
            while true:
                    parents ← PACK_INTERNALS(cur_level, level)
                    if len(parents) == 1:
                        return parents[0]  # root node
                    else:
                        cur_level ← parents  # go up one level
                        level += 1

*/

/*
FUNCTION PACK_INTERNALS(child_list, level, node_count) -> list of InternalNode :

if node_count < max_int_pointers:
    fill it all into one node
else
    make the divisions
    if child_nodes % max_int_keys == 0:
        num_nodes = child_nodes / max_int_children
        decide the splits -> every num_nodes multiple

    elif child_nodes % max_int_keys < min_int_keys but > 0:
        split is every num_nodes multiple except for the last two nodes
        settle the split for the last two nodes, borrow from the second last node
        num_borrow = min_int_keys - (child_nodes % max_int_keys)

    get all the first keys and node_id of all child nodes from index 1  -> end in the split
    for child in child_nodes:
        first_key = first_key(child_nodes)
        get node_id
        write into the internal node
    fill it in for all the nodes

return list of internal nodes for that level

*/