#include "bssearch_lib.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>

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

// Comparison function for qsort
int compare_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);  // Avoids floating point comparison issues
}

// Calculate statistics from an array of durations
void calculate_stats(double *durations, uint64_t n, search_stats_t *stats) {
    if (n == 0) {
        stats->min = 0;
        stats->max = 0;
        stats->avg = 0;
        stats->median = 0;
        stats->p90 = 0;
        stats->p95 = 0;
        stats->std_dev = 0;
        stats->iterations = 0;
        return;
    }

    // Sort the durations for percentile calculations
    qsort(durations, n, sizeof(double), compare_doubles);

    // Calculate min, max, and average
    stats->min = durations[0];
    stats->max = durations[n-1];

    double sum = 0;
    for (uint64_t i = 0; i < n; i++) {
        sum += durations[i];
    }
    stats->avg = sum / n;

    // Calculate median and percentiles
    if (n % 2 == 0) {
        stats->median = (durations[n/2 - 1] + durations[n/2]) / 2.0;
    } else {
        stats->median = durations[n/2];
    }

    // 90th percentile (index = 0.9 * n - 1)
    size_t p90_idx = (size_t)(0.9 * n);
    if (p90_idx >= n) p90_idx = n - 1;
    stats->p90 = durations[p90_idx];

    // 95th percentile (index = 0.95 * n - 1)
    size_t p95_idx = (size_t)(0.95 * n);
    if (p95_idx >= n) p95_idx = n - 1;
    stats->p95 = durations[p95_idx];

    // Calculate standard deviation
    double variance = 0.0;
    for (uint64_t i = 0; i < n; i++) {
        double diff = durations[i] - stats->avg;
        variance += diff * diff;
    }
    variance /= n;
    stats->std_dev = sqrt(variance);

    // Store number of iterations
    stats->iterations = n;
}