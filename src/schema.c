#include "schema.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

// this part defines the fixed-width packed record used on the disk
void schema_init_default(Schema *s)
{
    if (!s)
        return;
    s->n_fields = 6;

    // Field 0: GAME_ID (optional in CSV; we still keep 4 bytes in record)
    strncpy(s->fields[0].name, "GAME_ID", MAX_NAME);
    s->fields[0].type = F_INT32;
    s->fields[0].width = 4;

    // Field 1: GAME_DATE_EST (store first 10 chars)
    strncpy(s->fields[1].name, "GAME_DATE_EST", MAX_NAME);
    s->fields[1].type = F_CHAR;
    s->fields[1].width = 10;

    // Field 2: HOME_TEAM_ID (CSV may name this TEAM_ID_home)
    strncpy(s->fields[2].name, "HOME_TEAM_ID", MAX_NAME);
    s->fields[2].type = F_INT32;
    s->fields[2].width = 4;

    // Field 3: VISITOR_TEAM_ID (optional in your TSV; keep 4 bytes)
    strncpy(s->fields[3].name, "VISITOR_TEAM_ID", MAX_NAME);
    s->fields[3].type = F_INT32;
    s->fields[3].width = 4;

    // Field 4: FT_PCT_home
    strncpy(s->fields[4].name, "FT_PCT_home", MAX_NAME);
    s->fields[4].type = F_FLOAT;
    s->fields[4].width = 4;

    // Field 5: HOME_TEAM_WINS
    strncpy(s->fields[5].name, "HOME_TEAM_WINS", MAX_NAME);
    s->fields[5].type = F_BOOL;
    s->fields[5].width = 1;

    // compute packed record size
    s->record_size = 0;
    for (int i = 0; i < s->n_fields; i++)
        s->record_size += s->fields[i].width;
}

void schema_print(const Schema *s)
{
    if (!s)
        return;
    printf("Schema: %d fields, record_size=%u bytes\n", s->n_fields, s->record_size);
    for (int i = 0; i < s->n_fields; i++)
    {
        printf("  %s (type=%d, width=%u)\n",
               s->fields[i].name, s->fields[i].type, s->fields[i].width);
    }
}

// this part PACK/UNPACK between Row (decoded) and packed bytes (record_size)

void encode_row(const Schema *s, const Row *r, uint8_t *dst)
{
    if (!s || !r || !dst)
        return;
    uint8_t *p = dst;

    // GAME_ID (4byt)
    memcpy(p, &r->game_id, 4);
    p += 4;

    // GAME_DATE_EST (char[10])
    size_t len = strlen(r->game_date);
    memset(p, ' ', 10);
    memcpy(p, r->game_date, len > 10 ? 10 : len);
    p += 10;

    // HOME_TEAM_ID (4bytes)
    memcpy(p, &r->home_team_id, 4);
    p += 4;

    // VISITOR_TEAM_ID (4bytes)
    memcpy(p, &r->visitor_team_id, 4);
    p += 4;

    // FT_PCT_home (4bytes)
    memcpy(p, &r->ft_pct_home, 4);
    p += 4;

    // HOME_TEAM_WINS (1bytes)
    *p++ = r->home_team_wins ? 1 : 0;
}

void decode_row(const Schema *s, const uint8_t *src, Row *r)
{
    if (!s || !src || !r)
        return;
    const uint8_t *p = src;

    memcpy(&r->game_id, p, 4);
    p += 4;

    memcpy(r->game_date, p, 10);
    r->game_date[10] = '\0';
    p += 10;

    memcpy(&r->home_team_id, p, 4);
    p += 4;
    memcpy(&r->visitor_team_id, p, 4);
    p += 4;
    memcpy(&r->ft_pct_home, p, 4);
    p += 4;

    r->home_team_wins = *p++;
}

// this part is all the csv parser helpers logic for the csv parsing into the db
static void rstrip_inplace(char *s)
{
    size_t L = strlen(s);
    while (L && (s[L - 1] == '\n' || s[L - 1] == '\r' || s[L - 1] == ' ' || s[L - 1] == '\t'))
        s[--L] = '\0';
}
static char *lstrip(char *s)
{
    while (*s == ' ' || *s == '\t')
        s++;
    return s;
}
static void unquote_inplace(char *s)
{
    size_t L = strlen(s);
    if (L >= 2 && ((s[0] == '"' && s[L - 1] == '"') || (s[0] == '\'' && s[L - 1] == '\'')))
    {
        memmove(s, s + 1, L - 2);
        s[L - 2] = '\0';
    }
}
static void to_lower_inplace(char *s)
{
    for (; *s; ++s)
        *s = (char)tolower((unsigned char)*s);
}
static const char *skip_bom(const char *s)
{
    if ((unsigned char)s[0] == 0xEF && (unsigned char)s[1] == 0xBB && (unsigned char)s[2] == 0xBF)
        return s + 3;
    return s;
}
static void normalize_header(char *s)
{
    rstrip_inplace(s);
    char *p = lstrip(s);
    if (p != s)
        memmove(s, p, strlen(p) + 1);
    unquote_inplace(s);
    to_lower_inplace(s);
}
static int find_by_alias(char **cols_norm, int n, const char *const *aliases)
{
    for (int i = 0; i < n; i++)
    {
        for (int a = 0; aliases[a] != NULL; a++)
        {
            if (strcmp(cols_norm[i], aliases[a]) == 0)
                return i;
        }
    }
    return -1;
}

// this part maps the header and row parsing
int parse_header_map(const char *header_line_raw, CsvIdx *idx)
{
    if (!header_line_raw || !idx)
        return -1;

    char buf[8192];
    strncpy(buf, skip_bom(header_line_raw), sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    // split by comma OR tab
    char *cols_norm[512];
    char cells[512][256];
    int n = 0;
    char *save = NULL;
    char *tok = strtok_r(buf, ",\t\n", &save);
    while (tok && n < 512)
    {
        strncpy(cells[n], tok, sizeof(cells[n]) - 1);
        cells[n][sizeof(cells[n]) - 1] = '\0';
        normalize_header(cells[n]); // trim + unquote + lowercase
        cols_norm[n] = cells[n];
        n++;
        tok = strtok_r(NULL, ",\t\n", &save);
    }

    // alias lists (lowercase)
    const char *AL_GAME_ID[] = {"game_id", NULL};
    const char *AL_GAME_DATE_EST[] = {"game_date_est", "game_date", "date", "game_date_utc", NULL};
    const char *AL_HOME_TEAM_ID[] = {"home_team_id", "team_id_home", "home_id", NULL};
    const char *AL_VISITOR_TEAM_ID[] = {"visitor_team_id", "team_id_away", "away_team_id", "visitor_id", NULL};
    const char *AL_FT_PCT_HOME[] = {"ft_pct_home", "home_ft_pct", "ft_pct_h", NULL};
    const char *AL_HOME_TEAM_WINS[] = {"home_team_wins", "home_win", "is_home_win", "home_wins", NULL};

    idx->i_GAME_ID = find_by_alias(cols_norm, n, AL_GAME_ID);
    idx->i_GAME_DATE_EST = find_by_alias(cols_norm, n, AL_GAME_DATE_EST);
    idx->i_HOME_TEAM_ID = find_by_alias(cols_norm, n, AL_HOME_TEAM_ID);
    idx->i_VISITOR_TEAM_ID = find_by_alias(cols_norm, n, AL_VISITOR_TEAM_ID);
    idx->i_FT_PCT_home = find_by_alias(cols_norm, n, AL_FT_PCT_HOME);
    idx->i_HOME_TEAM_WINS = find_by_alias(cols_norm, n, AL_HOME_TEAM_WINS);

    // REQUIRED: present in your TSV; OPTIONAL: GAME_ID, VISITOR_TEAM_ID
    int ok =
        idx->i_GAME_DATE_EST >= 0 &&
        idx->i_HOME_TEAM_ID >= 0 &&
        idx->i_FT_PCT_home >= 0 &&
        idx->i_HOME_TEAM_WINS >= 0;

    if (!ok)
    {
        fprintf(stderr, "[header] normalized columns detected (%d):\n", n);
        for (int i = 0; i < n; i++)
            fprintf(stderr, "  [%d] %s\n", i, cols_norm[i]);
        return -1;
    }
    return 0;
}

int parse_row_by_index(const char *line_raw, const CsvIdx *id, Row *out)
{
    if (!line_raw || !id || !out)
        return -1;

    char line[8192];
    strncpy(line, skip_bom(line_raw), sizeof(line) - 1);
    line[sizeof(line) - 1] = '\0';
    rstrip_inplace(line);

    // split by comma or tab
    char *cols[512];
    int n = 0;
    char *save = NULL;
    char *tok = strtok_r(line, ",\t\n", &save);
    while (tok && n < 512)
    {
        char *cell = tok;
        cell = lstrip(cell);
        rstrip_inplace(cell);
        if (*cell == '"' || *cell == '\'')
        {
            size_t L = strlen(cell);
            if (L >= 2 && cell[L - 1] == cell[0])
            {
                cell[L - 1] = '\0';
                cell++;
            }
        }
        cols[n++] = cell;
        tok = strtok_r(NULL, ",\t\n", &save);
    }

    // require only the columns we marked as required in header map
    if (id->i_GAME_DATE_EST >= n || id->i_HOME_TEAM_ID >= n ||
        id->i_FT_PCT_home >= n || id->i_HOME_TEAM_WINS >= n)
    {
        return -1;
    }

    // optional presence flags if not the chck cannot go through
    int has_game_id = (id->i_GAME_ID >= 0 && id->i_GAME_ID < n);
    int has_vis_id = (id->i_VISITOR_TEAM_ID >= 0 && id->i_VISITOR_TEAM_ID < n);

    // parse into Row
    out->game_id = has_game_id ? atoi(cols[id->i_GAME_ID]) : 0;

    // keep first 10 characters of date
    const char *d = cols[id->i_GAME_DATE_EST];
    strncpy(out->game_date, d, 10);
    out->game_date[10] = '\0';

    out->home_team_id = atoi(cols[id->i_HOME_TEAM_ID]);
    out->visitor_team_id = has_vis_id ? atoi(cols[id->i_VISITOR_TEAM_ID]) : 0;
    out->ft_pct_home = (float)atof(cols[id->i_FT_PCT_home]);
    out->home_team_wins = (uint8_t)atoi(cols[id->i_HOME_TEAM_WINS]);

    return 0;
}
