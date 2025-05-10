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
    exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
    int implementation = 0;
    int num_threads = 32;
    int create_test = 0;
    size_t test_size = 1000000;
    uint64_t test_step = 10;
    int opt;
    const char *filepath = NULL;
    uint64_t target = 0;
    
    // Parse command-line options
    while ((opt = getopt(argc, argv, "i:t:cs:p:")) != -1) {
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
    
    // Run the selected implementation
    switch (implementation) {
        case 1:
            printf("Running simple mmap implementation...\n");
            return binary_search_uint64_mmap(filepath, target);
        case 2:
            printf("Running IO_uring implementation...\n");
            return binary_search_uint64(filepath, target);
        case 3:
            printf("Running parallel mmap implementation with %d threads...\n", num_threads);
            return parallel_binary_search_uint64_mmap(filepath, target, num_threads);
        default:
            fprintf(stderr, "Invalid implementation\n");
            return EXIT_FAILURE;
    }
}