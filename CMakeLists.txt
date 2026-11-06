cmake_minimum_required(VERSION 3.15)
project(message-queue LANGUAGES CXX)

option(MESSAGE_QUEUE_BLOCKING_RECEIVE "Receive messages blocking" OFF)
option(MESSAGE_QUEUE_MATCHED_RECV "Use MPI_Mrecv" OFF)
message(STATUS BLOCKING_RECEIVE ${MESSAGE_QUEUE_BLOCKING_RECEIVE})

find_package(MPI REQUIRED)
find_package(Boost REQUIRED COMPONENTS mpi )

#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address ")
#set (CMAKE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address ")


#set(GRAPH_IO_GENERATORS OFF)
if (NOT TARGET graph-io)
  add_subdirectory(extern/graph-io)
endif()

if (NOT TARGET backward)
  add_subdirectory(extern/backward-cpp)
endif()
if (NOT TARGET CLI11)
  add_subdirectory(extern/cli11)
endif()

add_library(message-queue INTERFACE)
target_include_directories(message-queue INTERFACE include)
target_link_libraries(message-queue INTERFACE MPI::MPI_CXX Boost::mpi)
target_compile_options(message-queue INTERFACE -std=c++17)
if (MESSAGE_QUEUE_BLOCKING_RECEIVE)
  target_compile_definitions(message-queue INTERFACE MESSAGE_QUEUE_BLOCKING_RECEIVE)
endif()
if (MESSAGE_QUEUE_MATCHED_RECV)
  target_compile_definitions(message-queue INTERFACE MESSAGE_QUEUE_MATCHED_RECV)
endif()

add_executable(example example.cpp)
target_link_libraries(example message-queue)

#add_executable(asynchronous asynchronous.cpp)
#target_link_libraries(asynchronous MPI::MPI_CXX)

add_executable(bfs bfs.cpp ${BACKWARD_ENABLE})
target_link_libraries(bfs message-queue graph-io CLI11::CLI11)
add_backward(bfs)
