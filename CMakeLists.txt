cmake_minimum_required(VERSION 3.24)

project(
  BriefKAsten
  DESCRIPTION
    "A C++ library for transparent, asynchronous, and lazy messaging using MPI"
  VERSION 0.2.0
  LANGUAGES CXX)

option(BRIEFKASTEN_BUILD_EXAMPLES "Build examples" ${PROJECT_IS_TOP_LEVEL})
option(BRIEFKASTEN_BUILD_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})
option(
  BRIEFKASTEN_USE_CXX23
  "Use C++23. Default is ON, when OFF, this library uses C++20 and depends on range-v3."
  ON)

find_package(MPI REQUIRED)

include(FetchContent)
FetchContent_Declare(
  kamping
  GIT_REPOSITORY https://github.com/kamping-site/kamping.git
  GIT_TAG packaging/update-kassert
  SYSTEM)

FetchContent_Declare(
  kassert
  GIT_REPOSITORY https://github.com/kamping-site/kassert.git
  GIT_TAG v1.0.0
  SYSTEM)

FetchContent_MakeAvailable(kamping kassert)

set(try_compile_version 20)
if(BRIEFKASTEN_USE_CXX23)
  set(try_compile_version 23)
endif()
try_compile(
  BRIEFKASTEN_HAS_LAZY_SPLIT_VIEW ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/cmake/compile_check.cpp CXX_STANDARD
          ${try_compile_version})

add_library(BriefKAsten_BriefKAsten INTERFACE)
target_sources(BriefKAsten_BriefKAsten INTERFACE FILE_SET HEADERS BASE_DIRS src)
add_subdirectory(src/briefkasten/)
target_link_libraries(BriefKAsten_BriefKAsten INTERFACE MPI::MPI_CXX
                                                        kassert::kassert)
target_link_libraries(BriefKAsten_BriefKAsten INTERFACE kamping::kamping)

if(BRIEFKASTEN_USE_CXX23)
  target_compile_features(BriefKAsten_BriefKAsten INTERFACE cxx_std_23)
else()
  target_compile_features(BriefKAsten_BriefKAsten INTERFACE cxx_std_20)
  message(STATUS "Using C++20 compatibility mode, adding dependency for range-v3")
  FetchContent_Declare(
    range-v3
    GIT_REPOSITORY https://github.com/ericniebler/range-v3
    GIT_TAG 0.12.0
    GIT_SHALLOW ON
    SYSTEM)
  FetchContent_MakeAvailable(range-v3)
  target_link_libraries(BriefKAsten_BriefKAsten INTERFACE range-v3::range-v3)
  target_compile_definitions(BriefKAsten_BriefKAsten INTERFACE BRIEFKASTEN_CXX20)
endif()

if(NOT BRIEFKASTEN_HAS_LAZY_SPLIT_VIEW)
  message(
    STATUS
      "Your compiler has std::views::split implementation which is lazy, see the caveats in the README"
  )
  target_compile_definitions(BriefKAsten
                             INTERFACE MESSAGE_QUEUE_SPLIT_VIEW_IS_LAZY)
else()
  message(
    STATUS "Compiler supports std::views::split and std::views::lazy_split")
endif()
add_library(BriefKAsten::BriefKAsten ALIAS BriefKAsten_BriefKAsten)

if(BRIEFKASTEN_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(BRIEFKASTEN_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
