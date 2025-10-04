#include "cli.h"
#include "heapfile.h"
#include "schema.h"
#include "build_bplus.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void usage()
{
    printf("Usage:\n");
    printf("  load <csv> <dbfile> [--buf N]\n");
    printf("  stats <dbfile> [--buf N]\n");
    printf("  scan  <dbfile> [--buf N] [--limit K]\n");
    printf("  build_bplus <dbfile> [--buf N]\n");
    printf("  delete_bplus <dbfile> <min_key> [--buf N]    # Delete records with FT_PCT_home > min_key\n");
}

int run_cli(int argc, char **argv)
{
    if (argc < 2)
    {
        usage();
        return 1;
    }
    int buf = 64, limit = 10;
    for (int i = 0; i < argc; i++)
    {
        if (strcmp(argv[i], "--buf") == 0 && i + 1 < argc)
            buf = atoi(argv[i + 1]);
        if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc)
            limit = atoi(argv[i + 1]);
    }
    if (strcmp(argv[1], "load") == 0 && argc >= 4)
    {
        const char *csv = argv[2];
        const char *db = argv[3];
        Schema s;
        schema_init_default(&s);
        HeapFile hf;
        if (hf_create(&hf, db, &s, buf) != 0)
        {
            fprintf(stderr, "create failed\n");
            return 2;
        }
        if (hf_load_csv(&hf, csv) != 0)
        {
            fprintf(stderr, "load failed\n");
            return 3;
        }
        hf_print_stats(&hf);
        hf_close(&hf);
        return 0;
    }
    else if (strcmp(argv[1], "stats") == 0 && argc >= 3)
    {
        const char *db = argv[2];
        HeapFile hf;
        if (hf_open(&hf, db, buf) != 0)
        {
            fprintf(stderr, "open failed\n");
            return 2;
        }
        hf_print_stats(&hf);
        hf_close(&hf);
        return 0;
    }
    else if (strcmp(argv[1], "scan") == 0 && argc >= 3)
    {
        const char *db = argv[2];
        HeapFile hf;
        if (hf_open(&hf, db, buf) != 0)
        {
            fprintf(stderr, "open failed\n");
            return 2;
        }
        hf_scan_print_firstN(&hf, limit);
        hf_close(&hf);
        return 0;
    }
    else if (strcmp(argv[1], "build_bplus") == 0 && argc >= 3)
    {
        const char *db = argv[2];
        HeapFile hf;
        if (hf_open(&hf, db, buf) != 0)
        {
            fprintf(stderr, "open failed\n");
            return 2;
        }
        if (scan_db(&hf) != 0){
            fprintf(stderr, "bplus build failed\n");
            return 3;
        }
        hf_close(&hf);
        return 0;
    }

    else if (strcmp(argv[1], "delete_bplus") == 0 && argc >= 4)
    {
        const char *db = argv[2];
        float min_key = atof(argv[3]);
        
        printf("=== B+ Tree Range Deletion ===\n");
        printf("Database: %s\n", db);
        printf("Deleting records with FT_PCT_home > %.6f\n", min_key);
        printf("========================================\n\n");
        
        extern int bptree_range_search_for_deletion(const char *btree_filename, float min_key, void *result);
        extern int bptree_perform_deletion(const char *db_filename, void *result);
        extern void run_comparison_tests();
        extern void cleanup_search_result(void *result);
        
        struct {
            void *records;
            size_t count;
            size_t capacity;
            uint32_t index_nodes_accessed;
            uint32_t leaf_nodes_accessed;
            double total_key_value;
            double search_time_ms;
        } search_result = {0};
        
        // Step 1: Search for records to delete (but don't delete yet)
        if (bptree_range_search_for_deletion("btree.db", min_key, &search_result) != 0) {
            fprintf(stderr, "B+ tree search for deletion failed\n");
            return 3;
        }
        
        // Step 2: Run performance comparison tests (while records still exist)
        printf("\n");
        run_comparison_tests();
        
        // Step 3: Actually delete the records and rebuild B+ tree
        if (bptree_perform_deletion(db, &search_result) != 0) {
            fprintf(stderr, "Record deletion failed\n");
            cleanup_search_result(&search_result);
            return 3;
        }
        
        // Step 4: Show updated B+ tree statistics
        printf("\n=== Updated B+ Tree Statistics ===\n");
        extern int btfm_open(void *btfm, const char *filename, size_t node_size);
        extern void btfm_close(void *btfm);
        extern int btfm_read_node(void *btfm, uint32_t node_id, void *node);
        
        // Open the B+ tree file to analyze the updated structure
        struct { char data[256]; } btfm;  // Placeholder for BtreeFileManager
        if (btfm_open(&btfm, "btree.db", 512) == 0) {
            uint32_t total_nodes = 0;
            uint32_t leaf_nodes = 0;
            uint8_t max_level = 0;
            uint32_t root_node_id = 0;
            
            // Count nodes and find root
            for (uint32_t test_id = 0; test_id < 1000; test_id++) {
                struct { uint8_t level; uint16_t key_count; float lower_bound; uint8_t bytes[500]; } test_node;
                if (btfm_read_node(&btfm, test_id, &test_node) == 0) {
                    total_nodes++;
                    if (test_node.level == 1) {
                        leaf_nodes++;
                    } else {
                        if (test_node.level > max_level) {
                            max_level = test_node.level;
                            root_node_id = test_id;
                        }
                    }
                } else {
                    break; // No more nodes
                }
            }
            
            printf("Total leaf nodes: %u\n", leaf_nodes);
            printf("Total nodes (incl. root): %u\n", total_nodes);
            printf("Number of levels: %u\n", max_level);
            
            btfm_close(&btfm);
        }
        printf("================================\n");
        
        // Clean up search results
        cleanup_search_result(&search_result);
        
        return 0;
    }
    else
    {
        usage();
        return 1;
    }
}
