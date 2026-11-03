cmake_minimum_required(VERSION 3.20)
project(duct
  VERSION 0.1.0
  DESCRIPTION "Aello: a minimal local IPC messaging runtime"
  LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(DUCT_BUILD_EXAMPLES "Build duct examples" ON)
option(DUCT_BUILD_TESTS "Build duct tests" ON)

add_library(duct
  src/address.cc
  src/duct.cc
  src/logging.cc
  src/message.cc
  src/qos_pipe.cc
  src/reconnect_pipe.cc
  src/socket_utils.cc
  src/shm_transport.cc
  src/tcp_transport.cc
  src/uds_transport.cc
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
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(duct PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/generated
)

# Generate the version header so consumers can query the library version.
configure_file(cmake/duct_version.h.in generated/duct/version.h @ONLY)
target_sources(duct PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/generated/duct/version.h)

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

option(DUCT_BUILD_BENCH "Build aello benchmarks" ON)
if (DUCT_BUILD_BENCH AND NOT WIN32)
  add_executable(aello_bench bench/aello_bench.cc)
  target_link_libraries(aello_bench PRIVATE duct)
  # Smoke-test the harness: small message count keeps CI fast.
  add_test(NAME aello_bench COMMAND aello_bench 200 64)
endif()

# ---- Install / packaging ----
set(DUCT_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/duct")

install(TARGETS duct
  EXPORT ductTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(DIRECTORY include/duct DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/generated/duct/version.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/duct)

install(EXPORT ductTargets
  FILE ductTargets.cmake
  NAMESPACE duct::
  DESTINATION ${DUCT_INSTALL_CMAKEDIR}
)

configure_package_config_file(
  cmake/ductConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/ductConfig.cmake
  INSTALL_DESTINATION ${DUCT_INSTALL_CMAKEDIR}
)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/ductConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/ductConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/ductConfigVersion.cmake
  DESTINATION ${DUCT_INSTALL_CMAKEDIR}
)

# pkg-config for non-CMake consumers.
configure_file(cmake/duct.pc.in ${CMAKE_CURRENT_BINARY_DIR}/duct.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/duct.pc
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

# Verify the install rules in CI: install to a temp prefix and confirm the
# package config, version header, and pkg-config file are produced.
if (DUCT_BUILD_TESTS)
  add_test(NAME duct_install
    COMMAND ${CMAKE_COMMAND} --install ${CMAKE_BINARY_DIR}
            --prefix ${CMAKE_BINARY_DIR}/install_prefix)
endif()

# CPack packages.
set(CPACK_PACKAGE_NAME "duct")
set(CPACK_PACKAGE_VENDOR "aello")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_FILE_NAME "duct-${PROJECT_VERSION}")
include(CPack)
