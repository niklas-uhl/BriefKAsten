include(GoogleTest)
FetchContent_Declare(
  KaTestrophe
  GIT_REPOSITORY https://github.com/kamping-site/KaTestrophe.cmake
  GIT_TAG v1.3.0)

FetchContent_MakeAvailable(KaTestrophe)

add_executable(alltoall_test alltoall_test.cpp)
target_link_libraries(alltoall_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(alltoall_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(alltoall_test CORES 1 2 3 4)
set_target_properties(alltoall_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(workloop_test workloop_test.cpp)
target_link_libraries(workloop_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(workloop_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(workloop_test CORES 1 2 3 4)
set_target_properties(workloop_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(idle_flush_test idle_flush_test.cpp)
target_link_libraries(idle_flush_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(idle_flush_test PRIVATE KaTestrophe::main)
# 8 and 9 ranks: the credit-pressure grid case needs them to fail if the idle flush ignored credits
katestrophe_add_mpi_test(idle_flush_test CORES 1 2 3 4 8 9)
set_target_properties(idle_flush_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(view_adaptor_test view_adaptor_test.cpp)
target_link_libraries(view_adaptor_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(view_adaptor_test PRIVATE GTest::gtest_main GTest::gmock)
gtest_discover_tests(view_adaptor_test)

add_executable(flow_control_test flow_control_test.cpp)
target_link_libraries(flow_control_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(flow_control_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(flow_control_test CORES 1 2 3 4)
set_target_properties(flow_control_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(large_message_test large_message_test.cpp)
target_link_libraries(large_message_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(large_message_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(large_message_test CORES 1 2 3 4 9)
set_target_properties(large_message_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(buffer_pool_test buffer_pool_test.cpp)
target_link_libraries(buffer_pool_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(buffer_pool_test PRIVATE KaTestrophe::main)
katestrophe_add_mpi_test(buffer_pool_test CORES 1 2 3 4 9)
set_target_properties(buffer_pool_test PROPERTIES KASSERT_ASSERTION_LEVEL 30)

add_executable(aggregators_test aggregators_test.cpp)
target_link_libraries(aggregators_test PRIVATE BriefKAsten::BriefKAsten)
target_link_libraries(aggregators_test PRIVATE GTest::gtest_main)
gtest_discover_tests(aggregators_test)
