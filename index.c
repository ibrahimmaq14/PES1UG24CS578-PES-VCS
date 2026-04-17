// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declaration - these are in object.c
void hash_to_hex(const ObjectID *id, char *hex_out);
int hex_to_hash(const char *hex, ObjectID *id_out);
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Helper for qsort to sort index entries by path
static int compare_index_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
//
// Identifies files that are staged, unstaged (modified/deleted in working dir),
// and untracked (present in working dir but not in index).
// Returns 0.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here. 
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── TODO: Implement these ───────────────────────────────────────────────────

// Load the index from .pes/index.
//
// HINTS - Useful functions:
//   - fopen (with "r"), fscanf, fclose : reading the text file line by line
//   - hex_to_hash                      : converting the parsed string to ObjectID
//
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    index->count = 0;
    
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // If index doesn't exist yet, that's OK (empty repository)
        return 0;
    }
    
    char hex_hash[HASH_HEX_SIZE + 1];
    char path[512];
    
    while (index->count < MAX_INDEX_ENTRIES) {
        // Parse format: "<mode> <64-char-hex-hash> <mtime> <size> <path>"
        int ret = fscanf(f, "%o %64s %" SCNu64 " %" SCNu32 " %511s\n",
                         &index->entries[index->count].mode,
                         hex_hash,
                         &index->entries[index->count].mtime_sec,
                         &index->entries[index->count].size,
                         path);
        
        if (ret != 5) {
            // End of file or parse error
            break;
        }
        
        // Convert hex string to hash
        if (hex_to_hash(hex_hash, &index->entries[index->count].hash) != 0) {
            fclose(f);
            return -1;
        }
        
        // Store the path
        strncpy(index->entries[index->count].path, path, sizeof(index->entries[index->count].path) - 1);
        index->entries[index->count].path[sizeof(index->entries[index->count].path) - 1] = '\0';
        
        index->count++;
    }
    
    fclose(f);
    return 0;
}

// Save the index to .pes/index atomically.
//
// HINTS - Useful functions and syscalls:
//   - qsort                            : sorting the entries array by path
//   - fopen (with "w"), fprintf        : writing to the temporary file
//   - hash_to_hex                      : converting ObjectID for text output
//   - fflush, fileno, fsync, fclose    : flushing userspace buffers and syncing to disk
//   - rename                           : atomically moving the temp file over the old index
//
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // Create a mutable copy to sort by path
    Index sorted_index = *index;
    
    // Sort entries by path
    qsort(sorted_index.entries, sorted_index.count, sizeof(IndexEntry),
          compare_index_entries);
    
    // Write to temporary file
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);
    
    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;
    
    char hex[HASH_HEX_SIZE + 1];
    for (int i = 0; i < sorted_index.count; i++) {
        hash_to_hex(&sorted_index.entries[i].hash, hex);
        fprintf(f, "%o %s %" PRIu64 " %" PRIu32 " %s\n",
                sorted_index.entries[i].mode,
                hex,
                sorted_index.entries[i].mtime_sec,
                sorted_index.entries[i].size,
                sorted_index.entries[i].path);
    }
    
    // Flush and sync
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    
    // Atomically rename
    return rename(tmp_path, INDEX_FILE);
}

// Stage a file for the next commit.
//
// HINTS - Useful functions and syscalls:
//   - fopen, fread, fclose             : reading the target file's contents
//   - object_write                     : saving the contents as OBJ_BLOB
//   - stat / lstat                     : getting file metadata (size, mtime, mode)
//   - index_find                       : checking if the file is already staged
//
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    // Open and read the file
    FILE *f = fopen(path, "rb");
    if (!f) {
        perror("Failed to open file");
        return -1;
    }
    
    // Get file size
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    if (file_size < 0) {
        fclose(f);
        return -1;
    }
    
    // Read file contents
    void *data = malloc((size_t)file_size);
    if (!data) {
        fclose(f);
        return -1;
    }
    
    if (fread(data, 1, (size_t)file_size, f) != (size_t)file_size) {
        free(data);
        fclose(f);
        return -1;
    }
    fclose(f);
    
    // Write file content as a blob
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, data, (size_t)file_size, &blob_id) != 0) {
        free(data);
        return -1;
    }
    free(data);
    
    // Get file metadata
    struct stat st;
    if (stat(path, &st) != 0) {
        return -1;
    }
    
    // Determine file mode
    uint32_t mode = 0100644; // Regular file, not executable
    if (st.st_mode & S_IXUSR) {
        mode = 0100755; // Executable
    }
    
    // Find or create index entry
    IndexEntry *entry = index_find(index, path);
    if (entry) {
        // Update existing entry
        entry->hash = blob_id;
        entry->mode = mode;
        entry->mtime_sec = (uint64_t)st.st_mtime;
        entry->size = (uint32_t)st.st_size;
    } else {
        // Add new entry
        if (index->count >= MAX_INDEX_ENTRIES) {
            return -1; // Index is full
        }
        entry = &index->entries[index->count];
        entry->hash = blob_id;
        entry->mode = mode;
        entry->mtime_sec = (uint64_t)st.st_mtime;
        entry->size = (uint32_t)st.st_size;
        strncpy(entry->path, path, sizeof(entry->path) - 1);
        entry->path[sizeof(entry->path) - 1] = '\0';
        index->count++;
    }
    
    // Save the updated index
    return index_save(index);
}