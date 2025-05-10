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
#include <pthread.h>

// Structure to pass data to threads
typedef struct {
    uint64_t *data;
    size_t start_idx;
    size_t end_idx;
    uint64_t target;
    int found;
    size_t found_idx;
    int num_comparisons;
} search_thread_data_t;

// Thread function for parallel binary search
void* binary_search_thread(void *arg) {
    search_thread_data_t *thread_data = (search_thread_data_t *)arg;
    
    size_t lo = thread_data->start_idx;
    size_t hi = thread_data->end_idx;
    uint64_t target = thread_data->target;
    uint64_t *data = thread_data->data;
    thread_data->found = 0;
    thread_data->num_comparisons = 0;
    
    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        thread_data->num_comparisons++;
        
        if (data[mid] == target) {
            thread_data->found = 1;
            thread_data->found_idx = mid;
            break;
        } else if (data[mid] < target) {
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    
    pthread_exit(NULL);
}

// Parallel binary search for a target uint64_t in a file using mmap
int parallel_binary_search_uint64_mmap(const char *filepath, uint64_t target, int num_threads) {
    int fd;
    uint64_t *data;
    struct stat st;
    size_t num_elements;
    int found = 0;
    off_t target_offset = -1;
    uint64_t start_time, end_time;
    int total_comparisons = 0;
    pthread_t *threads;
    search_thread_data_t *thread_data;
    
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
    
    printf("Searching for value %" PRIu64 " in file with %zu elements using %d threads\n", 
           target, num_elements, num_threads);
    
    // Memory map the file
    data = (uint64_t *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return -1;
    }
    int err = madvise(data, st.st_size, MADV_RANDOM);
    if (err != 0) {
        perror("madvise");
        munmap(data, st.st_size);
        close(fd);
        return -1;
    }
    
    // Adjust number of threads if necessary based on data size
    if (num_elements < (size_t)num_threads) {
        num_threads = num_elements;
        printf("Adjusted number of threads to %d based on data size\n", num_threads);
    }
    
    // Allocate memory for threads and thread data
    threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    thread_data = (search_thread_data_t *)malloc(num_threads * sizeof(search_thread_data_t));
    
    if (!threads || !thread_data) {
        perror("malloc");
        munmap(data, st.st_size);
        close(fd);
        free(threads);
        free(thread_data);
        return -1;
    }
    
    // Divide data among threads
    size_t elements_per_thread = num_elements / num_threads;
    size_t remainder = num_elements % num_threads;
    
    // Create threads
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].data = data;
        thread_data[i].target = target;
        
        // Calculate start and end indices for this thread
        thread_data[i].start_idx = i * elements_per_thread;
        thread_data[i].end_idx = (i + 1) * elements_per_thread - 1;
        
        // Add remainder elements to the last thread
        if (i == num_threads - 1) {
            thread_data[i].end_idx += remainder;
        }
        
        // Create thread
        if (pthread_create(&threads[i], NULL, binary_search_thread, &thread_data[i]) != 0) {
            perror("pthread_create");
            munmap(data, st.st_size);
            close(fd);
            free(threads);
            free(thread_data);
            return -1;
        }
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        total_comparisons += thread_data[i].num_comparisons;
        
        // Check if target was found in this thread
        if (thread_data[i].found) {
            found = 1;
            target_offset = thread_data[i].found_idx * sizeof(uint64_t);
        }
    }
    
    // Clean up
    free(threads);
    free(thread_data);
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
    printf("Search statistics (parallel mmap with %d threads):\n", num_threads);
    printf("  Total time: %.3f ms\n", elapsed_ms);
    printf("  Total comparisons performed: %d\n", total_comparisons);

    return 0;
}