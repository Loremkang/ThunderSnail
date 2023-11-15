cmake_minimum_required(VERSION 3.16.0)

project(ThunderSnail LANGUAGES C)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct SRC_COMMON_BASE_STRUCT)

add_executable(host testdriver.c ${SRC_COMMON_BASE_STRUCT})
target_include_directories(host PRIVATE . ${CMAKE_CURRENT_SOURCE_DIR}/common_base_struct)