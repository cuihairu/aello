cmake_minimum_required(VERSION 3.20)
project(duct LANGUAGES CXX)

option(DUCT_BUILD_EXAMPLES "Build duct examples" ON)
option(DUCT_BUILD_TESTS "Build duct tests" ON)

add_library(duct
  src/address.cc
  src/duct.cc
  src/message.cc
  src/qos_pipe.cc
  src/socket_utils.cc
  src/shm_transport.cc
  src/tcp_transport.cc
  src/wire.cc
)

# Add Windows-specific implementations
if(WIN32)
  target_sources(duct PRIVATE
    src/win_shm.cc
    src/pipe_transport.cc
  )
endif()

target_compile_features(duct PUBLIC cxx_std_20)
target_include_directories(duct PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

if (MSVC)
  target_compile_options(duct PRIVATE /W4)
  # Enable UTF-8 source/execution charset (needed for non-ASCII comments/strings on MSVC).
  target_compile_options(duct PUBLIC /utf-8)
else()
  target_compile_options(duct PRIVATE -Wall -Wextra -Wpedantic)
endif()

if (WIN32)
  target_link_libraries(duct PUBLIC ws2_32)
endif()

if (DUCT_BUILD_EXAMPLES)
  add_executable(duct_echo_server examples/duct_echo_server.cc)
  target_link_libraries(duct_echo_server PRIVATE duct)

  add_executable(duct_echo_client examples/duct_echo_client.cc)
  target_link_libraries(duct_echo_client PRIVATE duct)

  add_executable(qos_test examples/qos_test.cc)
  target_link_libraries(qos_test PRIVATE duct)

  add_executable(shm_test examples/shm_test.cc)
  target_link_libraries(shm_test PRIVATE duct)

  add_executable(pipe_test examples/pipe_test.cc)
  target_link_libraries(pipe_test PRIVATE duct)
endif()

if (DUCT_BUILD_TESTS)
  enable_testing()
  add_executable(duct_tests
    tests/duct_tests.cc
  )
  target_link_libraries(duct_tests PRIVATE duct)
  add_test(NAME duct_tests COMMAND duct_tests)
endif()
