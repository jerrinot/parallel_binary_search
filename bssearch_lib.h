#ifndef BSSEARCH_LIB_H
#define BSSEARCH_LIB_H

#include <stdint.h>
#include <stddef.h>

// Common utility functions
uint64_t get_microseconds();
int create_test_file(const char *filepath, size_t num_elements, uint64_t step);

// Implementation interfaces
int binary_search_uint64_mmap(const char *filepath, uint64_t target);
int binary_search_uint64(const char *filepath, uint64_t target);
int parallel_binary_search_uint64_mmap(const char *filepath, uint64_t target, int num_threads);

#endif // BSSEARCH_LIB_H