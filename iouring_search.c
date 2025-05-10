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
#include <sys/uio.h>         // For struct iovec
#include <linux/fs.h>        // For RWF_* flags

#define QUEUE_DEPTH 64       // How many operations we can queue at once
#define PARALLEL_READS 4     // Number of speculative reads to perform
#define BATCH_SIZE PARALLEL_READS  // Process this many completions at once
#define BUFFER_SIZE (sizeof(uint64_t))
#define READAHEAD_THRESHOLD 512  // When range is smaller than this many elements, use readahead
#define READAHEAD_SIZE (READAHEAD_THRESHOLD * sizeof(uint64_t)) // Size in bytes to readahead

// When range is smaller than this, use linear search instead of continuing binary search
// This threshold is chosen based on the tradeoff between:
// - Cost of I/O operations (higher cost = higher threshold is better)
// - Cost of in-memory scanning (higher cost = lower threshold is better)
// - Cache line size considerations (typically 64 bytes)
// For uint64_t values (8 bytes each), 32 elements = 256 bytes = 4 cache lines
// Linear search is faster for small ranges because it:
// 1. Reduces the number of I/O operations by reading all data at once
// 2. Takes advantage of sequential memory access patterns
// 3. Avoids the binary search overhead when the range is small
#define LINEAR_SEARCH_THRESHOLD 0

typedef struct {
    off_t offset;           // File offset for this read
    uint64_t value;         // The uint64_t value read
    int index;              // Which search position this represents
    int valid;              // Whether a valid value was read
} read_data;

// Binary search for a target uint64_t in a file of sorted uint64_t values
int binary_search_uint64(const char *filepath, uint64_t target, int use_sqpoll, int use_buffers, int use_readahead) {
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    read_data reads[PARALLEL_READS];
    int ret, found = 0;
    off_t target_offset = -1;
    struct stat st;
    uint64_t start_time, end_time;
    int total_reads = 0;
    int buffers_registered = 0;  // Track if buffers are registered
    
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
    
    // Initialize io_uring - optionally with SQPOLL flag for kernel thread polling
    int sqpoll_enabled = 0;

    if (use_sqpoll) {
        // Try to initialize with SQPOLL
        struct io_uring_params params = {0};
        params.flags = IORING_SETUP_SQPOLL;  // Use kernel polling thread to avoid syscalls
        params.sq_thread_idle = 2000;        // Thread idles for 2 seconds before going to sleep

        ret = io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params);
        if (ret < 0) {
            int saved_errno = errno;
            printf("Note: SQPOLL io_uring mode requires root privileges (error %d: %s)\n",
                  saved_errno, strerror(saved_errno));
            printf("Falling back to standard IO_uring mode...\n");

            // Fall back to standard mode
            ret = io_uring_queue_init(QUEUE_DEPTH, &ring, 0);
            if (ret < 0) {
                perror("io_uring_queue_init");
                close(fd);
                return -1;
            }
        } else {
            sqpoll_enabled = 1;
            printf("SQPOLL io_uring mode enabled (kernel polling)\n");
        }
    } else {
        // Standard mode requested
        ret = io_uring_queue_init(QUEUE_DEPTH, &ring, 0);
        if (ret < 0) {
            perror("io_uring_queue_init");
            close(fd);
            return -1;
        }
    }
    
    // Allocate memory for read buffers
    for (int i = 0; i < PARALLEL_READS; i++) {
        reads[i].valid = 0;
    }

    // Register buffers if requested
    if (use_buffers) {
        // Prepare an array of iovec structures for buffer registration
        struct iovec iov[PARALLEL_READS];

        // Initialize each iovec to point to our read buffers
        for (int i = 0; i < PARALLEL_READS; i++) {
            iov[i].iov_base = &reads[i].value;
            iov[i].iov_len = sizeof(uint64_t);
        }

        // Register the buffers with io_uring
        ret = io_uring_register_buffers(&ring, iov, PARALLEL_READS);
        if (ret < 0) {
            printf("Note: Failed to register buffers with io_uring (error %d: %s)\n",
                   errno, strerror(errno));
            printf("Falling back to standard buffer mode...\n");
        } else {
            buffers_registered = 1;
            printf("Buffer registration enabled (memory-to-kernel zero-copy)\n");
        }
    }
    
    // Main search loop
    while (lo <= hi) {
        // Calculate positions for parallel reads
        off_t range = hi - lo;

        // Check if the range is small enough for linear search and optimizations are enabled
        if (use_readahead && range <= LINEAR_SEARCH_THRESHOLD) {
            printf("Switching to linear search for range [%lld-%lld] (%lld elements)\n",
                  (long long)lo, (long long)hi, (long long)(range + 1));

            // Allocate buffer for the whole range
            uint64_t *buffer = malloc((range + 1) * sizeof(uint64_t));
            if (!buffer) {
                perror("malloc for linear search buffer");
                goto cleanup;
            }

            // Read the entire range at once
            ssize_t bytes_read = pread(fd, buffer, (range + 1) * sizeof(uint64_t), lo * sizeof(uint64_t));
            if (bytes_read < 0) {
                perror("pread for linear search");
                free(buffer);
                goto cleanup;
            }

            // Linear scan through the buffer
            for (off_t i = 0; i <= range; i++) {
                if (buffer[i] == target) {
                    found = 1;
                    target_offset = (lo + i) * sizeof(uint64_t);
                    printf("Linear search found target at index %lld\n", (long long)(lo + i));
                    break;
                }
            }

            // Clean up and break from the main loop
            free(buffer);
            break;
        }

        // Check if the range is small enough for readahead and readahead is enabled
        if (use_readahead && range <= READAHEAD_THRESHOLD) {
            // Small range - use posix_fadvise to prefetch the entire range
            off_t readahead_offset = lo * sizeof(uint64_t);
            size_t readahead_size = (range + 1) * sizeof(uint64_t);  // +1 to include both ends

            // Use posix_fadvise with POSIX_FADV_WILLNEED to prefetch data
            ret = posix_fadvise(fd, readahead_offset, readahead_size, POSIX_FADV_WILLNEED);
            if (ret < 0) {
                // Readahead failed - just continue with normal reads
                // This is not critical, so we don't goto cleanup
                perror("posix_fadvise readahead");
            } else {
                // Print diagnostic message
                printf("Readahead issued for range [%lld-%lld] (%zu bytes)\n",
                      (long long)lo, (long long)hi, readahead_size);
            }
        }

        // If range is small, reduce number of reads
        int active_reads;
        if (range > PARALLEL_READS * 100) {
            active_reads = PARALLEL_READS;
        } else {
            active_reads = 1;
        }

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

            // Prepare read operation using registered buffers if available
            if (buffers_registered) {
                // Use fixed buffers for zero-copy I/O
                io_uring_prep_read_fixed(sqe, fd, &reads[i].value, sizeof(uint64_t),
                                        reads[i].offset, i);  // i is the buffer index
            } else {
                // Standard read
                io_uring_prep_read(sqe, fd, &reads[i].value, sizeof(uint64_t), reads[i].offset);
            }

            // Set user data to identify this request later
            io_uring_sqe_set_data(sqe, &reads[i]);
        }
        
        // Count the reads we're performing
        total_reads += active_reads;
        
        // Submit and wait for all operations at once
        ret = io_uring_submit_and_wait(&ring, active_reads);
        if (ret < 0) {
            perror("io_uring_submit_and_wait");
            goto cleanup;
        }

        // Process all completions in a batch
        int head;
        struct io_uring_cqe *cqes[BATCH_SIZE];
        int count = io_uring_peek_batch_cqe(&ring, cqes, active_reads);

        // Process the batch of completions
        for (int i = 0; i < count; i++) {
            cqe = cqes[i];

            // Get the read_data associated with this completion
            read_data *data = io_uring_cqe_get_data(cqe);
            if (!data) continue; // Skip if no data (shouldn't happen for these reads)

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
        }

        // Mark all CQEs as seen at once
        io_uring_cq_advance(&ring, count);
        
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

        // When lo == hi, the next iteration will handle it correctly through the main loop
        // with active_reads = 1, no special case needed
    }
    
cleanup:
    // Unregister buffers if they were registered
    if (buffers_registered) {
        io_uring_unregister_buffers(&ring);
    }

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
    printf("  IO_uring mode: %s%s\n",
           sqpoll_enabled ? "SQPOLL (kernel polling)" : "Standard",
           buffers_registered ? " with buffer registration" : "");

    return found ? 0 : -1;
}