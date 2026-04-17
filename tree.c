// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <inttypes.h>

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// Forward declaration (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1; 
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = tree->count * 296; 
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        
        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to step over the null terminator written by sprintf
        
        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Minimal index entry struct (local to tree.c to avoid dependency on index.h)
typedef struct {
    uint32_t mode;
    ObjectID hash;
    uint64_t mtime_sec;
    uint32_t size;
    char path[512];
} LocalIndexEntry;

// Helper: recursively build tree from entries filtered by prefix
static int build_tree_recursive(LocalIndexEntry *filtered_entries, int count, ObjectID *id_out) {
    Tree tree;
    tree.count = 0;
    
    int i = 0;
    while (i < count && tree.count < MAX_TREE_ENTRIES) {
        const char *path = filtered_entries[i].path;
        const char *slash = strchr(path, '/');
        
        if (!slash) {
            // This is a file (no more slashes)
            tree.entries[tree.count].mode = filtered_entries[i].mode;
            tree.entries[tree.count].hash = filtered_entries[i].hash;
            strcpy(tree.entries[tree.count].name, path);
            tree.count++;
            i++;
        } else {
            // This is a directory - collect all entries for this directory
            size_t dir_len = slash - path;
            char dir_name[256];
            memcpy(dir_name, path, dir_len);
            dir_name[dir_len] = '\0';
            
            // Check if already added
            int already_added = 0;
            for (int j = 0; j < tree.count; j++) {
                if (strcmp(tree.entries[j].name, dir_name) == 0) {
                    already_added = 1;
                    break;
                }
            }
            
            if (!already_added) {
                // Collect all entries in this directory
                LocalIndexEntry subentries[MAX_TREE_ENTRIES];
                int subcount = 0;
                
                for (int j = i; j < count; j++) {
                    if (strncmp(filtered_entries[j].path, dir_name, dir_len) == 0 &&
                        filtered_entries[j].path[dir_len] == '/') {
                        // Strip the directory prefix
                        LocalIndexEntry sub = filtered_entries[j];
                        strcpy(sub.path, filtered_entries[j].path + dir_len + 1);
                        subentries[subcount++] = sub;
                    }
                }
                
                // Recursively build subtree
                ObjectID subid;
                if (build_tree_recursive(subentries, subcount, &subid) != 0) {
                    return -1;
                }
                
                // Add directory to tree
                tree.entries[tree.count].mode = MODE_DIR;
                tree.entries[tree.count].hash = subid;
                strcpy(tree.entries[tree.count].name, dir_name);
                tree.count++;
                
                // Skip all entries in this directory
                while (i < count && 
                       strncmp(filtered_entries[i].path, dir_name, dir_len) == 0 &&
                       filtered_entries[i].path[dir_len] == '/') {
                    i++;
                }
            } else {
                i++;
            }
        }
    }
    
    // Serialize and write tree
    void *data;
    size_t len;
    if (tree_serialize(&tree, &data, &len) != 0) {
        return -1;
    }
    
    int result = object_write(OBJ_TREE, data, len, id_out);
    free(data);
    return result;
}

// Forward declaration (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// Forward declarations for hash conversion (implemented in object.c)
void hash_to_hex(const ObjectID *id, char *hex_out);
int hex_to_hash(const char *hex, ObjectID *id_out);

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// HINTS - Useful functions and concepts for this phase:
//   - index_load      : load the staged files into memory
//   - strchr          : find the first '/' in a path to separate directories from files
//   - strncmp         : compare prefixes to group files belonging to the same subdirectory
//   - Recursion       : you will likely want to create a recursive helper function 
//                       (e.g., `write_tree_level(entries, count, depth)`) to handle nested dirs.
//   - tree_serialize  : convert your populated Tree struct into a binary buffer
//   - object_write    : save that binary buffer to the store as OBJ_TREE
//
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    // Load the index from .pes/index directly
    LocalIndexEntry entries[4096]; // Local entries array
    int entry_count = 0;
    
    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // If index doesn't exist, create an empty tree
        Tree empty_tree;
        empty_tree.count = 0;
        void *data;
        size_t len;
        if (tree_serialize(&empty_tree, &data, &len) != 0) {
            return -1;
        }
        int result = object_write(OBJ_TREE, data, len, id_out);
        free(data);
        return result;
    }
    
    char hex_hash[HASH_HEX_SIZE + 1];
    char path[512];
    
    while (entry_count < 4096) {
        int ret = fscanf(f, "%o %64s %" SCNu64 " %" SCNu32 " %511s\n",
                         &entries[entry_count].mode,
                         hex_hash,
                         &entries[entry_count].mtime_sec,
                         &entries[entry_count].size,
                         path);
        
        if (ret != 5) {
            break; // EOF or parse error
        }
        
        if (hex_to_hash(hex_hash, &entries[entry_count].hash) != 0) {
            fclose(f);
            return -1;
        }
        
        strncpy(entries[entry_count].path, path, sizeof(entries[entry_count].path) - 1);
        entries[entry_count].path[sizeof(entries[entry_count].path) - 1] = '\0';
        
        entry_count++;
    }
    
    fclose(f);
    
    // Build tree hierarchy from all entries
    return build_tree_recursive(entries, entry_count, id_out);
}