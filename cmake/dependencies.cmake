include(FetchContent)

FetchContent_Declare(
  kamping
  GIT_REPOSITORY https://github.com/kamping-site/kamping.git
  GIT_TAG v0.1.1
  SYSTEM
)

FetchContent_Declare(
  kassert
  GIT_REPOSITORY https://github.com/kamping-site/kassert.git
  GIT_TAG f0873f8
  SYSTEM
)

FetchContent_Declare(
  range-v3
  URL https://github.com/ericniebler/range-v3/archive/0.12.0.zip
  SYSTEM
  SOURCE_SUBDIR NON_EXISTANT
)

# if(NOT EXISTS ${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/range-v3-extra.cmake AND
#     NOT EXISTS ${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/range-v3Extra.cmake)
#   file(WRITE ${CMAKE_FIND_PACKAGE_REDIRECTS_DIR}/range-v3-extra.cmake
# [=[
#     add_library(range-v3 INTERFACE IMPORTED)
#     target_include_directories(range-v3
#                                INTERFACE ${range-v3_SOURCE_DIR}/include)
#     add_library(range-v3::range-v3 ALIAS range-v3)
# ]=])
# endif()

FetchContent_Declare(
  CLI11
  GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
  GIT_TAG v2.5.0
  SYSTEM
)

FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 10.1.1
  SYSTEM
)

# FetchContent_Declare(
#   bakward-mpi
#   GIT_REPOSITORY https://github.com/kamping-site/bakward-mpi.git
#   GIT_TAG 89de113
#   SYSTEM
# )
