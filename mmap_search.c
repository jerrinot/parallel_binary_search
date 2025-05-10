#include "bssearch_lib.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <math.h>

// Binary search for a target uint64_t in a file using mmap
int binary_search_uint64_mmap(const char *filepath, uint64_t target) {
    int fd;
    uint64_t *data;
    struct stat st;
    size_t num_elements;
    int found = 0;
    off_t target_offset = -1;
    uint64_t start_time, end_time;
    int total_comparisons = 0;
    
    // Start timing
    start_time = get_microseconds();
    
    // Open the file
    fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return -1;
    }
    
    // Get file size
    if (fstat(fd, &st) < 0) {
        perror("fstat");
        close(fd);
        return -1;
    }
    
    // Check if file size is valid for uint64_t array
    if (st.st_size % sizeof(uint64_t) != 0) {
        fprintf(stderr, "File size is not aligned with uint64_t size\n");
        close(fd);
        return -1;
    }
    
    // Calculate number of elements
    num_elements = st.st_size / sizeof(uint64_t);
    if (num_elements == 0) {
        fprintf(stderr, "File is empty\n");
        close(fd);
        return -1;
    }
    
    printf("Searching for value %" PRIu64 " in file with %zu elements\n", target, num_elements);
    
    // Memory map the file
    data = (uint64_t *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return -1;
    }
    
    // Perform binary search on the memory-mapped data
    size_t lo = 0;
    size_t hi = num_elements - 1;
    
    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        total_comparisons++;
        
        if (data[mid] == target) {
            found = 1;
            target_offset = mid * sizeof(uint64_t);
            break;
        }

        if (data[mid] < target) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    
    // Clean up
    munmap(data, st.st_size);
    close(fd);
    
    // End timing
    end_time = get_microseconds();
    double elapsed_ms = (end_time - start_time) / 1000.0;
    
    if (found) {
        printf("Found uint64_t value %" PRIu64 " at offset %lld (element index %lld)\n", 
               target, (long long)target_offset, (long long)(target_offset / sizeof(uint64_t)));
    } else {
        printf("uint64_t value %" PRIu64 " not found in file\n", target);
    }
    
    // Print timing information
    printf("Search statistics (mmap):\n");
    printf("  Total time: %.3f ms\n", elapsed_ms);
    printf("  Total comparisons performed: %d\n", total_comparisons);

    return 0;
}