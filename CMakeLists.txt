cmake_minimum_required(VERSION 3.16.0)

project(ThunderSnail)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

# For googletest
include(FetchContent)
FetchContent_Declare(
  googletest
  # Specify the commit you depend on and update it regularly.
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
FetchContent_MakeAvailable(googletest)

set(CMAKE_VERBOSE_MAKEFILE true)

set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct SRC_COMMON_BASE_STRUCT)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/newlink SRC_NEWLINK)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/protocol SRC_PROTOCOL)

add_executable(host testdriver.c ${SRC_COMMON_BASE_STRUCT} ${SRC_PROTOCOL} ${SRC_NEWLINK})
target_include_directories(host PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct)
target_include_directories(host PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/protocol)
target_include_directories(host PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/newlink)

# Build tests:
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/tests SRC_TEST)
add_executable(test ${SRC_TEST} ${SRC_COMMON_BASE_STRUCT} ${SRC_NEWLINK})
target_include_directories(test PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct)
target_include_directories(test PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/protocol)
target_include_directories(test PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/newlink)
target_link_libraries(test gtest_main)
add_test(NAME example_test COMMAND test)

set(UPMEM_SDK_DIR "$ENV{UPMEM_SDK_DIR}")
if(NOT UPMEM_SDK_DIR)
    message(FATAL_ERROR "environment variable UPMEM_SDK_DIR is not set")
    exit()
endif()

set(UPMEM_C_COMPILER ${UPMEM_SDK_DIR}/bin/dpu-upmem-dpurte-clang)

if(NOT EXECUTABLE_OUTPUT_PATH)
    set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
endif()

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/dpu DPU_SOURCES)

file(GLOB DPU_HEADERS "dpu/*.h")

set(DPU_TASK_NAME dpu_task)
add_custom_target(${DPU_TASK_NAME} ALL
    COMMAND ${UPMEM_C_COMPILER} -O3 -fgnu89-inline
            -DNR_TASKLETS=${NR_TASKLETS}
            -DSTACK_SIZE_DEFAULT=1024
            -I${CMAKE_CURRENT_SOURCE_DIR}/dpu
            ${DPU_SOURCES}
            -o ${EXECUTABLE_OUTPUT_PATH}/${DPU_TASK_NAME}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${DPU_SOURCES}
            ${DPU_HEADERS}
    VERBATIM)

install(PROGRAMS ${EXECUTABLE_OUTPUT_PATH}/${DPU_TASK_NAME} DESTINATION bin)
