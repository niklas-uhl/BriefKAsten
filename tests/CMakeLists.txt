include(GoogleTest)
FetchContent_Declare(
  KaTestrophe
  GIT_REPOSITORY https://github.com/kamping-site/KaTestrophe.cmake
  GIT_TAG v1.0.1)

FetchContent_MakeAvailable(KaTestrophe)

add_executable(alltoall_test alltoall_test.cpp)
target_link_libraries(alltoall_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(alltoall_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(alltoall_test CORES 1 2 4)
set_target_properties(alltoall_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(workloop_test workloop_test.cpp)
target_link_libraries(workloop_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(workloop_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(workloop_test CORES 1 2 4)
set_target_properties(workloop_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(view_adaptor_test view_adaptor_test.cpp)
target_link_libraries(view_adaptor_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(view_adaptor_test PRIVATE GTest::gtest_main GTest::gmock)
gtest_discover_tests(view_adaptor_test)
