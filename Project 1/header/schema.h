#ifndef SCHEMA_H
#define SCHEMA_H
#include <stdint.h>
#include <stdio.h>

#define MAX_FIELDS 32
#define MAX_NAME   32

typedef enum { F_INT32=1, F_FLOAT=2, F_BOOL=3, F_CHAR=4 } FieldType;

typedef struct {
    char      name[MAX_NAME];
    FieldType type;
    uint16_t  width;   // bytes for this field in the packed record
} Field;

typedef struct {
    Field    fields[MAX_FIELDS];
    uint16_t n_fields;
    uint16_t record_size; // sum(width)
} Schema;

// Row = decoded values for printing/debug (simple variant for Task 1)
typedef struct {
    int32_t  game_id;
    char     game_date[11]; // 10 + '\0'
    int32_t  home_team_id;
    int32_t  visitor_team_id;
    float    ft_pct_home;
    uint8_t  home_team_wins;
} Row;

typedef struct {
    int i_GAME_ID;
    int i_GAME_DATE_EST;
    int i_HOME_TEAM_ID;
    int i_VISITOR_TEAM_ID;
    int i_FT_PCT_home;
    int i_HOME_TEAM_WINS;
} CsvIdx;

int parse_header_map(const char* header_line, CsvIdx* idx);
int parse_row_by_index(const char* line, const CsvIdx* idx, Row* out);


void schema_init_default(Schema* s);  // fills with the 6 fields above
void schema_print(const Schema* s);

// pack/unpack one row to/from raw record bytes
void encode_row(const Schema* s, const Row* r, uint8_t* dst /*size record_size*/);
void decode_row(const Schema* s, const uint8_t* src, Row* r);

// quick CSV -> Row (very simple, assumes known header order)
int parse_row_from_csv_line(const char* line, Row* out);

#endif
