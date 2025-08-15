cmake_minimum_required(VERSION 3.24)

project(BriefKAsten
  DESCRIPTION "A C++ library for transparent, asynchronous, and lazy messaging using MPI"
  VERSION 0.2.0
  LANGUAGES CXX)

option(MESSAGE_QUEUE_BUILD_EXAMPLES "Build examples" ${PROJECT_IS_TOP_LEVEL})
option(MESSAGE_QUEUE_BUILD_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})

include(cmake/dependencies.cmake)

find_package(MPI REQUIRED)

include(cmake/setup_assertion_level.cmake)
FetchContent_MakeAvailable(kassert kamping)

try_compile(
  MESSAGE_QUEUE_HAS_LAZY_SPLIT_VIEW ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/cmake/compile_check.cpp CXX_STANDARD 23)

add_library(message-queue INTERFACE)
target_sources(message-queue INTERFACE FILE_SET HEADERS BASE_DIRS src)
add_subdirectory(src/message-queue/)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX kassert::kassert)
target_link_libraries(message-queue INTERFACE kamping::kamping)

target_compile_features(message-queue INTERFACE cxx_std_23)
if(NOT MESSAGE_QUEUE_HAS_LAZY_SPLIT_VIEW)
  message(
    STATUS
      "Your compiler has std::views::split implementation which is lazy, see the caveats in the README"
  )
  target_compile_definitions(message-queue
                             INTERFACE MESSAGE_QUEUE_SPLIT_VIEW_IS_LAZY)
else()
  message(
    STATUS "Compiler supports std::views::split and std::views::lazy_split")
endif()
add_library(message-queue::message-queue ALIAS message-queue)

if(MESSAGE_QUEUE_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(MESSAGE_QUEUE_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
