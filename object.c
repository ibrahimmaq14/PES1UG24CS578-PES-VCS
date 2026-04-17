// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Steps:
//   1. Build the full object: header ("blob 16\0") + data
//   2. Compute SHA-256 hash of the FULL object (header + data)
//   3. Check if object already exists (deduplication) — if so, just return success
//   4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//   5. Write to a temporary file in the same shard directory
//   6. fsync() the temporary file to ensure data reaches disk
//   7. rename() the temp file to the final path (atomic on POSIX)
//   8. Open and fsync() the shard directory to persist the rename
//   9. Store the computed hash in *id_out

// HINTS - Useful syscalls and functions for this phase:
//   - sprintf / snprintf : formatting the header string
//   - compute_hash       : hashing the combined header + data
//   - object_exists      : checking for deduplication
//   - mkdir              : creating the shard directory (use mode 0755)
//   - open, write, close : creating and writing to the temp file
//                          (Use O_CREAT | O_WRONLY | O_TRUNC, mode 0644)
//   - fsync              : flushing the file descriptor to disk
//   - rename             : atomically moving the temp file to the final path
//

//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Map type to string
    const char *type_str;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob"; break;
        case OBJ_TREE:   type_str = "tree"; break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }
    
    // Step 1: Build the header
    char header[64];
    snprintf(header, sizeof(header), "%s %zu", type_str, len);
    
    // Step 2: Compute hash of header + null byte + data
    size_t full_len = strlen(header) + 1 + len; // +1 for null byte
    uint8_t *full_data = malloc(full_len);
    if (!full_data) return -1;
    
    memcpy(full_data, header, strlen(header) + 1); // copy header with null byte
    memcpy(full_data + strlen(header) + 1, data, len); // copy data after null
    
    compute_hash(full_data, full_len, id_out);
    
    // Step 3: Check if object already exists
    if (object_exists(id_out)) {
        free(full_data);
        return 0; // Deduplication: already exists
    }
    
    // Step 4: Create shard directory if needed
    char shard_dir[512];
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    
    mkdir(shard_dir, 0755); // Ignore error if already exists
    
    // Step 5: Write to temporary file
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/.tmp_obj", shard_dir);
    
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_data);
        return -1;
    }
    
    // Write full object (header + null + data)
    if (write(fd, full_data, full_len) != (ssize_t)full_len) {
        close(fd);
        unlink(tmp_path);
        free(full_data);
        return -1;
    }
    
    // Step 6: fsync the temporary file
    if (fsync(fd) != 0) {
        close(fd);
        unlink(tmp_path);
        free(full_data);
        return -1;
    }
    close(fd);
    
    // Step 7: Rename temp file to final path atomically
    char final_path[512];
    object_path(id_out, final_path, sizeof(final_path));
    
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        free(full_data);
        return -1;
    }
    
    // Step 8: fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }
    
    free(full_data);
    return 0;
}

// Read an object from the store.
//
// Steps:
//   1. Build the file path from the hash using object_path()
//   2. Open and read the entire file
//   3. Parse the header to extract the type string and size
//   4. Verify integrity: recompute the SHA-256 of the file contents
//      and compare to the expected hash (from *id). Return -1 if mismatch.
//   5. Set *type_out to the parsed ObjectType
//   6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// HINTS - Useful syscalls and functions for this phase:
//   - object_path        : getting the target file path
//   - fopen, fread, fseek: reading the file into memory
//   - memchr             : safely finding the '\0' separating header and data
//   - strncmp            : parsing the type string ("blob", "tree", "commit")
//   - compute_hash       : re-hashing the read data for integrity verification
//   - memcmp             : comparing the computed hash against the requested hash
//   - malloc, memcpy     : allocating and returning the extracted data
//
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Get the file path
    char path[512];
    object_path(id, path, sizeof(path));
    
    // Step 2: Open and read the entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    
    // Get file size
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    if (file_size < 0) {
        fclose(f);
        return -1;
    }
    
    uint8_t *file_data = malloc((size_t)file_size);
    if (!file_data) {
        fclose(f);
        return -1;
    }
    
    if (fread(file_data, 1, (size_t)file_size, f) != (size_t)file_size) {
        free(file_data);
        fclose(f);
        return -1;
    }
    fclose(f);
    
    // Step 3: Parse the header
    // Find the null byte separating header from data
    const uint8_t *null_ptr = memchr(file_data, '\0', (size_t)file_size);
    if (!null_ptr) {
        free(file_data);
        return -1; // No null byte found
    }
    
    size_t header_len = null_ptr - file_data;
    char header_str[64];
    if (header_len >= sizeof(header_str)) {
        free(file_data);
        return -1;
    }
    memcpy(header_str, file_data, header_len);
    header_str[header_len] = '\0';
    
    // Parse type and size from header
    char type_str[16];
    size_t claimed_size;
    if (sscanf(header_str, "%15s %zu", type_str, &claimed_size) != 2) {
        free(file_data);
        return -1;
    }
    
    // Determine ObjectType
    if (strcmp(type_str, "blob") == 0) {
        *type_out = OBJ_BLOB;
    } else if (strcmp(type_str, "tree") == 0) {
        *type_out = OBJ_TREE;
    } else if (strcmp(type_str, "commit") == 0) {
        *type_out = OBJ_COMMIT;
    } else {
        free(file_data);
        return -1;
    }
    
    // Step 4: Verify integrity by recomputing hash
    ObjectID computed_id;
    compute_hash(file_data, (size_t)file_size, &computed_id);
    
    if (memcmp(computed_id.hash, id->hash, HASH_SIZE) != 0) {
        free(file_data);
        return -1; // Hash mismatch
    }
    
    // Step 5 & 6: Extract and return the data portion
    size_t data_offset = header_len + 1; // +1 to skip null byte
    size_t data_len = (size_t)file_size - data_offset;
    
    void *result = malloc(data_len);
    if (!result) {
        free(file_data);
        return -1;
    }
    
    if (data_len > 0) {
        memcpy(result, file_data + data_offset, data_len);
    }
    
    *data_out = result;
    *len_out = data_len;
    
    free(file_data);
    return 0;
}
