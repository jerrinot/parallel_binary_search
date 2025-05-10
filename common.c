#include "bssearch_lib.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

// Function to get current time in microseconds
uint64_t get_microseconds() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

// Function to create a test file with sorted uint64_t values
int create_test_file(const char *filepath, size_t num_elements, uint64_t step) {
    FILE *file = fopen(filepath, "wb");
    if (!file) {
        perror("fopen");
        return -1;
    }
    
    printf("Creating test file with %zu elements...\n", num_elements);
    
    for (uint64_t i = 0; i < num_elements; i++) {
        uint64_t value = i * step;
        if (fwrite(&value, sizeof(uint64_t), 1, file) != 1) {
            perror("fwrite");
            fclose(file);
            return -1;
        }
    }
    
    fclose(file);
    printf("Test file created successfully: %s\n", filepath);
    return 0;
}