cmake_minimum_required(VERSION 3.16.0)

project(ThunderSnail LANGUAGES C)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct SRC_COMMON_BASE_STRUCT)

add_executable(host testdriver.c ${SRC_COMMON_BASE_STRUCT})
target_include_directories(host PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct)


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
