#ifndef BSSEARCH_LIB_H
#define BSSEARCH_LIB_H

#include <stdint.h>
#include <stddef.h>

// Structure to hold statistics
typedef struct {
    double min;           // Minimum duration in milliseconds
    double max;           // Maximum duration in milliseconds
    double avg;           // Average duration in milliseconds
    double median;        // Median duration in milliseconds
    double p90;           // 90th percentile duration in milliseconds
    double p95;           // 95th percentile duration in milliseconds
    double std_dev;       // Standard deviation of durations
    uint64_t iterations;  // Number of iterations
} search_stats_t;

// Common utility functions
uint64_t get_microseconds();
int create_test_file(const char *filepath, size_t num_elements, uint64_t step);
void calculate_stats(double *durations, uint64_t n, search_stats_t *stats);
int compare_doubles(const void *a, const void *b);

// Implementation interfaces
int binary_search_uint64_mmap(const char *filepath, uint64_t target);
int binary_search_uint64(const char *filepath, uint64_t target, int use_sqpoll, int use_buffers);
int parallel_binary_search_uint64_mmap(const char *filepath, uint64_t target, int num_threads);

#endif // BSSEARCH_LIB_H