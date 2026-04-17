#include "index.h"
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declarations
void hash_to_hex(const ObjectID *id, char *hex_out);
int hex_to_hash(const char *hex, ObjectID *id_out);
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ─────────────────────────────────────────

static int compare_index_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            memmove(&index->entries[i], &index->entries[i + 1],
                    (index->count - i - 1) * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");

    if (index->count == 0) {
        printf("  (nothing to show)\n");
    } else {
        for (int i = 0; i < index->count; i++) {
            printf("  staged:     %s\n", index->entries[i].path);
        }
    }

    printf("\nUnstaged changes:\n  (nothing to show)\n");
    printf("\nUntracked files:\n  (nothing to show)\n\n");

    return 0;
}

// ─── IMPLEMENTATION ───────────────────────────────────

// FIXED: safe load
int index_load(Index *index) {
    if (!index) return -1;

    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0;

    char hex[HASH_HEX_SIZE + 1];
    char path[512];

    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *e = &index->entries[index->count];

        int ret = fscanf(f, "%o %64s %" SCNu64 " %" SCNu32 " %511s",
                         &e->mode, hex, &e->mtime_sec, &e->size, path);

        if (ret != 5) break;

        if (hex_to_hash(hex, &e->hash) != 0) {
            fclose(f);
            return -1;
        }

        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';

        index->count++;
    }

    fclose(f);
    return 0;
}

// FIXED: atomic save
int index_save(const Index *index) {
    // Copy index to heap to avoid heavy stack usage
    Index *tmp = malloc(sizeof(Index));
    if (!tmp) return -1;
    *tmp = *index;

    qsort(tmp->entries, tmp->count, sizeof(IndexEntry), compare_index_entries);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);

    FILE *f = fopen(tmp_path, "w");
    if (!f) { free(tmp); return -1; }

    char hex[HASH_HEX_SIZE + 1];

    for (int i = 0; i < tmp->count; i++) {
        hash_to_hex(&tmp->entries[i].hash, hex);

        fprintf(f, "%o %s %" PRIu64 " %" PRIu32 " %s\n",
                tmp->entries[i].mode,
                hex,
                tmp->entries[i].mtime_sec,
                tmp->entries[i].size,
                tmp->entries[i].path);
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);

    int rc = rename(tmp_path, INDEX_FILE);
    free(tmp);
    return rc;
}

// FIXED: no segfault
int index_add(Index *index, const char *path) {
    if (!index) return -1;

    // 🔥 IMPORTANT: initialize before use
    if (index_load(index) < 0) return -1;

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (size < 0) {
        fclose(f);
        return -1;
    }

    void *data = malloc(size);
    if (!data) {
        fclose(f);
        return -1;
    }

    if (fread(data, 1, size, f) != (size_t)size) {
        free(data);
        fclose(f);
        return -1;
    }

    fclose(f);

    ObjectID id;
    if (object_write(OBJ_BLOB, data, size, &id) != 0) {
        free(data);
        return -1;
    }

    free(data);

    struct stat st;
    if (stat(path, &st) != 0) return -1;

    IndexEntry *e = index_find(index, path);

    if (!e) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        e = &index->entries[index->count++];
    }

    e->hash = id;
    e->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    e->mtime_sec = (uint64_t)st.st_mtime;
    e->size = (uint32_t)st.st_size;

    strncpy(e->path, path, sizeof(e->path) - 1);
    e->path[sizeof(e->path) - 1] = '\0';

    return index_save(index);
}
