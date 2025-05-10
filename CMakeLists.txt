cmake_minimum_required(VERSION 3.16)
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

# Create executable with all new implementation files
add_executable(parallel_binary_search
    launcher.c
    common.c
    mmap_search.c
    iouring_search.c
    parallel_mmap_search.c)

# Link against liburing and pthread
target_link_libraries(parallel_binary_search ${LIBURING_LIBRARY} pthread m)

# Add inttypes.h support for PRIu64 macro
target_compile_definitions(parallel_binary_search PRIVATE __STDC_FORMAT_MACROS)

# Output compiler flags for debugging
message(STATUS "LIBURING_INCLUDE_DIR: ${LIBURING_INCLUDE_DIR}")
message(STATUS "LIBURING_LIBRARY: ${LIBURING_LIBRARY}")