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
    // minhwan: Production-ready command for B+ tree range deletion
    else if (strcmp(argv[1], "delete_bplus") == 0 && argc >= 4)
    {
        const char *db = argv[2];
        float min_key = atof(argv[3]);  // minhwan: Parse minimum key value from command line
        
        printf("=== B+ Tree Range Deletion ===\n");
        printf("Database: %s\n", db);
        printf("Deleting records with FT_PCT_home > %.6f\n", min_key);
        printf("========================================\n\n");
        
        extern int bptree_range_delete(const char *db_filename, const char *btree_filename, float min_key, void *result);  // minhwan: Declare external function
        
        // Use a simple structure to capture results without exposing internal details
        struct {
            void *records;
            size_t count;
            size_t capacity;
            uint32_t index_nodes_accessed;
            uint32_t leaf_nodes_accessed;
            double total_key_value;
            double search_time_ms;
        } search_result = {0};
        
        // Perform actual B+ tree range deletion (search + delete + rebuild)
        if (bptree_range_delete(db, "btree.db", min_key, &search_result) != 0) {
            fprintf(stderr, "B+ tree range deletion failed\n");
            return 3;
        }
        
        // Clean up search results
        extern void cleanup_search_result(void *result);
        cleanup_search_result(&search_result);
        
        // Run comparison test between B+ tree and linear scan
        printf("\n");
        extern void run_comparison_tests(); // minhwan: Declare comparison test function
        run_comparison_tests();
        
        return 0;
    }
    else
    {
        usage();
        return 1;
    }
}
