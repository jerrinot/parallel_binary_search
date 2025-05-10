#include "bssearch_lib.h"
#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <sys/stat.h>
#include <inttypes.h>

#define QUEUE_DEPTH 64       // How many operations we can queue at once
#define PARALLEL_READS 8     // Number of speculative reads to perform
#define BUFFER_SIZE (sizeof(uint64_t))

typedef struct {
    off_t offset;           // File offset for this read
    uint64_t value;         // The uint64_t value read
    int index;              // Which search position this represents
    int valid;              // Whether a valid value was read
} read_data;

// Binary search for a target uint64_t in a file of sorted uint64_t values
int binary_search_uint64(const char *filepath, uint64_t target) {
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    read_data reads[PARALLEL_READS];
    int ret, found = 0;
    off_t target_offset = -1;
    struct stat st;
    uint64_t start_time, end_time;
    int total_reads = 0;
    
    // Open the file
    int fd = open(filepath, O_RDONLY);
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

   if (posix_fadvise(fd, 0, st.st_size, POSIX_FADV_RANDOM) < 0) {
        fprintf(stderr, "Could not fadvise\n");
        close(fd);
        return -1;
    }
    
    // Calculate number of elements
    size_t num_elements = st.st_size / sizeof(uint64_t);
    if (num_elements == 0) {
        fprintf(stderr, "File is empty\n");
        close(fd);
        return -1;
    }
    
    printf("Searching for value %" PRIu64 " in file with %zu elements\n", target, num_elements);

    // Start timing
    start_time = get_microseconds();
    
    // Initialize search boundaries
    off_t lo = 0;
    off_t hi = num_elements - 1;
    
    // Initialize io_uring
    ret = io_uring_queue_init(QUEUE_DEPTH, &ring, 0);
    if (ret < 0) {
        perror("io_uring_queue_init");
        close(fd);
        return -1;
    }
    
    // Allocate memory for read buffers
    for (int i = 0; i < PARALLEL_READS; i++) {
        reads[i].valid = 0;
    }
    
    // Main search loop
    while (lo <= hi) {
        // Calculate positions for parallel reads
        off_t range = hi - lo;
        
        // If range is small, reduce number of reads
        int active_reads;
        if (range > PARALLEL_READS * 100) {
            active_reads = PARALLEL_READS;
        } else {
            active_reads = 1;
        }


        if (active_reads <= 0) active_reads = 1;  // Always read at least one value
        
        off_t step = range / (active_reads + 1);
        step = (step == 0) ? 1 : step;  // Ensure minimum step size
        
        // Prepare read requests
        for (int i = 0; i < active_reads; i++) {
            // Calculate the position to read
            off_t index_pos = lo + step * (i + 1);
            if (index_pos > hi) index_pos = hi;
            
            reads[i].offset = index_pos * sizeof(uint64_t);
            reads[i].index = i;
            reads[i].valid = 0;
            
            // Get an SQE
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                fprintf(stderr, "Could not get SQE\n");
                goto cleanup;
            }
            
            // Prepare read operation
            io_uring_prep_read(sqe, fd, &reads[i].value, sizeof(uint64_t), reads[i].offset);
            
            // Set user data to identify this request later
            io_uring_sqe_set_data(sqe, &reads[i]);
        }
        
        // Count the reads we're performing
        total_reads += active_reads;
        
        // Submit all read operations at once
        ret = io_uring_submit(&ring);
        if (ret < 0) {
            perror("io_uring_submit");
            goto cleanup;
        }
        
        // Process results as they complete
        for (int i = 0; i < active_reads; i++) {
            ret = io_uring_wait_cqe(&ring, &cqe);
            if (ret < 0) {
                perror("io_uring_wait_cqe");
                goto cleanup;
            }
            
            // Get the read_data associated with this completion
            read_data *data = io_uring_cqe_get_data(cqe);
            
            // Check if operation succeeded
            if (cqe->res == sizeof(uint64_t)) {
                data->valid = 1;
                
                // Compare with target
                if (data->value == target) {
                    // Found it!
                    found = 1;
                    target_offset = data->offset;
                } 
            } else if (cqe->res < 0) {
                fprintf(stderr, "Read failed: %s\n", strerror(-cqe->res));
            }
            
            // Mark this CQE as seen
            io_uring_cqe_seen(&ring, cqe);
        }
        
        // If we found the target, we're done
        if (found) {
            break;
        }
        
        // Otherwise, adjust search boundaries based on read values
        off_t new_lo = lo;
        off_t new_hi = hi;
        
        for (int i = 0; i < active_reads; i++) {
            if (!reads[i].valid) continue;
            
            off_t elem_idx = reads[i].offset / sizeof(uint64_t);
            
            if (reads[i].value == target) {
                // Found it!
                found = 1;
                target_offset = reads[i].offset;
                break;
            }

            if (reads[i].value < target) {
                // Target is in the upper half
                if (elem_idx + 1 > new_lo) {
                    new_lo = elem_idx + 1;
                }
            } else {
                // Target is in the lower half
                if (elem_idx - 1 < new_hi) {
                    new_hi = elem_idx - 1;
                }
            }
        }
        
        if (found) break;
        
        // Update boundaries for next iteration
        lo = new_lo;
        hi = new_hi;
        
        // If boundaries are the same and we haven't found it, do one direct read
        if (lo == hi) {
            uint64_t value;
            off_t offset = lo * sizeof(uint64_t);
            
            // Get an SQE for the final read
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) {
                fprintf(stderr, "Could not get SQE for final read\n");
                goto cleanup;
            }
            
            // Prepare the final read
            io_uring_prep_read(sqe, fd, &value, sizeof(uint64_t), offset);
            io_uring_sqe_set_data(sqe, NULL);
            
            // Count this final read
            total_reads++;
            
            // Submit and wait
            ret = io_uring_submit(&ring);
            if (ret < 0) {
                perror("io_uring_submit final read");
                goto cleanup;
            }
            
            ret = io_uring_wait_cqe(&ring, &cqe);
            if (ret < 0) {
                perror("io_uring_wait_cqe final read");
                goto cleanup;
            }
            
            if (cqe->res == sizeof(uint64_t)) {
                if (value == target) {
                    found = 1;
                    target_offset = offset;
                }
            }
            
            io_uring_cqe_seen(&ring, cqe);
            break;  // We're done searching
        }
    }
    
cleanup:
    // Clean up io_uring
    io_uring_queue_exit(&ring);
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
    printf("Search statistics:\n");
    printf("  Total time: %.3f ms\n", elapsed_ms);
    printf("  Total reads performed: %d\n", total_reads);
    printf("  Average time per read: %.3f ms\n", elapsed_ms / total_reads);
    printf("  Total bytes read: %zu\n", total_reads * sizeof(uint64_t));

    return found ? 0 : -1;
}