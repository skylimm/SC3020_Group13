#include "cli.h"
#include "heapfile.h"
#include "schema.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void usage()
{
    printf("Usage:\n");
    printf("  load <csv> <dbfile> [--buf N]\n");
    printf("  stats <dbfile> [--buf N]\n");
    printf("  scan  <dbfile> [--buf N] [--limit K]\n");
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
    else
    {
        usage();
        return 1;
    }
}
