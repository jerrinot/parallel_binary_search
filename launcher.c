#include "bssearch_lib.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>

// Print usage information
void print_usage(const char *program_name) {
    fprintf(stderr, "Usage: %s -i <implementation> <filepath> <target_uint64> [options]\n", program_name);
    fprintf(stderr, "  -i <implementation>: Implementation to use (required)\n");
    fprintf(stderr, "      1 = Simple mmap (main_mmap.c)\n");
    fprintf(stderr, "      2 = IO_uring (main_iouring.c)\n");
    fprintf(stderr, "      3 = Parallel mmap (main_parallel_mmap.c)\n");
    fprintf(stderr, "  <filepath>: Path to the file to search in\n");
    fprintf(stderr, "  <target_uint64>: Value to search for\n");
    fprintf(stderr, "  Options:\n");
    fprintf(stderr, "    -t <num_threads>: Number of threads (for implementation 3 only, default: 32)\n");
    fprintf(stderr, "    -c: Create test file\n");
    fprintf(stderr, "    -s <size>: Number of elements in test file (default: 1000000)\n");
    fprintf(stderr, "    -p <step>: Step between values in test file (default: 10)\n");
    fprintf(stderr, "    -d: Drop caches before running (requires sudo permissions)\n");
    fprintf(stderr, "    -n <iterations>: Number of iterations to run (default: 1)\n");
    fprintf(stderr, "    -q: Use SQPOLL mode for IO_uring (requires root privileges, implementation 2 only)\n");
    exit(EXIT_FAILURE);
}

// Print statistics
void print_stats(const search_stats_t *stats, const char *impl_name) {
    printf("\nStatistics for %s (%ld iterations):\n", impl_name, stats->iterations);
    printf("  Min time:     %.3f ms\n", stats->min);
    printf("  Max time:     %.3f ms\n", stats->max);
    printf("  Avg time:     %.3f ms\n", stats->avg);
    printf("  Median time:  %.3f ms\n", stats->median);
    printf("  90th %%tile:   %.3f ms\n", stats->p90);
    printf("  95th %%tile:   %.3f ms\n", stats->p95);
    printf("  Std Dev:      %.3f ms\n", stats->std_dev);
}

// Function to run a single search iteration and measure time
int run_iteration(int implementation, const char *filepath, uint64_t target, int num_threads, int drop_caches, int use_sqpoll, double *duration) {
    int ret = 0;
    uint64_t start_time, end_time;

    // Drop caches if requested (before timing starts)
    if (drop_caches) {
        int status = system("sudo ./drop_caches.sh > /dev/null 2>&1");
        if (status != 0) {
            fprintf(stderr, "Failed to drop caches. Make sure drop_caches.sh is executable and you have sudo permissions.\n");
            return -1;
        }
    }

    // Start timing
    start_time = get_microseconds();

    // Run the selected implementation
    switch (implementation) {
        case 1:
            ret = binary_search_uint64_mmap(filepath, target);
            break;
        case 2:
            ret = binary_search_uint64(filepath, target, use_sqpoll);
            break;
        case 3:
            ret = parallel_binary_search_uint64_mmap(filepath, target, num_threads);
            break;
        default:
            fprintf(stderr, "Invalid implementation\n");
            return -1;
    }
    
    // End timing
    end_time = get_microseconds();
    
    // Calculate duration in milliseconds
    *duration = (end_time - start_time) / 1000.0;
    
    return ret;
}

int main(int argc, char *argv[]) {
    int implementation = 0;
    int num_threads = 32;
    int create_test = 0;
    int drop_caches = 0;
    int use_sqpoll = 0;  // Default to not using SQPOLL
    size_t test_size = 1000000;
    uint64_t test_step = 10;
    uint64_t iterations = 1;  // Default to 1 iteration
    int opt;
    const char *filepath = NULL;
    uint64_t target = 0;
    
    // Parse command-line options
    while ((opt = getopt(argc, argv, "i:t:cs:p:dn:q")) != -1) {
        switch (opt) {
            case 'i':
                implementation = atoi(optarg);
                if (implementation < 1 || implementation > 3) {
                    fprintf(stderr, "Invalid implementation: %d\n", implementation);
                    print_usage(argv[0]);
                }
                break;
            case 't':
                num_threads = atoi(optarg);
                if (num_threads <= 0) {
                    fprintf(stderr, "Number of threads must be positive\n");
                    print_usage(argv[0]);
                }
                break;
            case 'c':
                create_test = 1;
                break;
            case 's':
                test_size = strtoull(optarg, NULL, 10);
                if (test_size == 0) {
                    fprintf(stderr, "Test file size must be positive\n");
                    print_usage(argv[0]);
                }
                break;
            case 'p':
                test_step = strtoull(optarg, NULL, 10);
                break;
            case 'd':
                drop_caches = 1;
                break;
            case 'n':
                iterations = strtoull(optarg, NULL, 10);
                if (iterations == 0) {
                    fprintf(stderr, "Number of iterations must be positive\n");
                    print_usage(argv[0]);
                }
                break;
            case 'q':
                use_sqpoll = 1; // Enable SQPOLL mode for IO_uring
                break;
            default:
                print_usage(argv[0]);
        }
    }
    
    // Check if we have implementation specified
    if (implementation == 0) {
        fprintf(stderr, "Implementation (-i) is required\n");
        print_usage(argv[0]);
    }
    
    // Check if we have enough positional arguments
    if (optind + 1 >= argc) {
        fprintf(stderr, "Not enough arguments\n");
        print_usage(argv[0]);
    }
    
    // Get filepath and target value
    filepath = argv[optind];
    target = strtoull(argv[optind + 1], NULL, 10);
    
    // Check for conversion errors
    if (errno == ERANGE) {
        perror("strtoull");
        return EXIT_FAILURE;
    }
    
    // Create test file if requested
    if (create_test) {
        printf("Creating test file with %zu elements...\n", test_size);
        if (create_test_file(filepath, test_size, test_step) != 0) {
            return EXIT_FAILURE;
        }
    }
    
    // Print information about the run
    printf("Running search with the following parameters:\n");
    printf("  Implementation: ");
    switch (implementation) {
        case 1:
            printf("Simple mmap\n");
            break;
        case 2:
            printf("IO_uring%s\n", use_sqpoll ? " with SQPOLL" : "");
            break;
        case 3:
            printf("Parallel mmap with %d threads\n", num_threads);
            break;
    }
    printf("  File: %s\n", filepath);
    printf("  Target value: %" PRIu64 "\n", target);
    printf("  Iterations: %" PRIu64 "\n", iterations);
    printf("  Drop caches: %s\n", drop_caches ? "Yes" : "No");
    
    // Allocate memory for storing durations
    double *durations = (double *)malloc(iterations * sizeof(double));
    if (!durations) {
        perror("malloc");
        return EXIT_FAILURE;
    }
    
    // Show initial drop cache message if needed
    if (drop_caches) {
        printf("Dropping caches before each iteration (requires sudo)...\n");
    }
    
    // Run iterations
    int ret = 0;
    printf("Running %ld iterations...\n", iterations);
    
    for (uint64_t i = 0; i < iterations; i++) {
        // Show progress every 10% of iterations
        if (iterations > 10 && i % (iterations / 10) == 0) {
            printf("  Progress: %ld%%\n", (i * 100) / iterations);
        }
        
        // Run a single iteration
        double duration;
        int iter_ret = run_iteration(implementation, filepath, target, num_threads, drop_caches, use_sqpoll, &duration);
        
        // Store the duration
        durations[i] = duration;
        
        // If there's an error, report it and exit
        if (iter_ret < 0) {
            ret = iter_ret;
            break;
        }
    }
    
    // Calculate and print statistics
    if (ret >= 0) {
        search_stats_t stats;
        calculate_stats(durations, iterations, &stats);
        
        // Get implementation name for stats display
        const char *impl_name;
        switch (implementation) {
            case 1: impl_name = "Simple mmap"; break;
            case 2: impl_name = "IO_uring"; break;
            case 3: 
                {
                    char buffer[50];
                    snprintf(buffer, sizeof(buffer), "Parallel mmap (%d threads)", num_threads);
                    impl_name = buffer;
                }
                break;
            default: impl_name = "Unknown implementation"; break;
        }
        
        print_stats(&stats, impl_name);
    }
    
    // Clean up
    free(durations);
    
    return ret;
}