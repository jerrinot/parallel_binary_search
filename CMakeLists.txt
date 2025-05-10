cmake_minimum_required(VERSION 3.31)
project(parallel_binary_search C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS_RELEASE "-O3 -march=native -flto -fomit-frame-pointer")
set(CMAKE_BUILD_TYPE Release)

# Find liburing package
find_path(LIBURING_INCLUDE_DIR liburing.h)
find_library(LIBURING_LIBRARY uring)

# Check if liburing was found
if(NOT LIBURING_LIBRARY)
    message(FATAL_ERROR "liburing library not found. Please install liburing-dev package.")
endif()

# Include directories
include_directories(${LIBURING_INCLUDE_DIR})

# Create executable
add_executable(parallel_binary_search main_iouring.c
        main_mmap.c.bak
        main_parallel_mmap.c.bak)

# Link against liburing
target_link_libraries(parallel_binary_search ${LIBURING_LIBRARY})

# Add inttypes.h support for PRIu64 macro
target_compile_definitions(parallel_binary_search PRIVATE __STDC_FORMAT_MACROS)

# Add missing header include in the source
set_source_files_properties(main_iouring.c PROPERTIES COMPILE_FLAGS "-include inttypes.h")

# Output compiler flags for debugging
message(STATUS "LIBURING_INCLUDE_DIR: ${LIBURING_INCLUDE_DIR}")
message(STATUS "LIBURING_LIBRARY: ${LIBURING_LIBRARY}")