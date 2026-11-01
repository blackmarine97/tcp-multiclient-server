cmake_minimum_required(VERSION 3.16)
project(radar_sim VERSION 1.0 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "radar_sim uses POSIX sockets/poll(): build it on Linux (or WSL).")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

option(RADAR_BUILD_TESTS "Build unit and integration tests" ON)
set(RADAR_SANITIZER "" CACHE STRING "Optional sanitizer: thread | address")

if(RADAR_SANITIZER)
  add_compile_options(-fsanitize=${RADAR_SANITIZER} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${RADAR_SANITIZER})
endif()

find_package(Threads REQUIRED)

add_library(radar_core STATIC
  src/config.cpp
  src/line_buffer.cpp
  src/log.cpp
  src/protocol.cpp
  src/server.cpp
  src/track_frame.cpp
)
target_include_directories(radar_core PUBLIC src)
target_link_libraries(radar_core PUBLIC Threads::Threads)
target_compile_options(radar_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(radar_server src/main.cpp)
target_link_libraries(radar_server PRIVATE radar_core)
target_compile_options(radar_server PRIVATE -Wall -Wextra -Wpedantic)

add_executable(radar_client tools/radar_client.cpp)
target_link_libraries(radar_client PRIVATE radar_core)
target_compile_options(radar_client PRIVATE -Wall -Wextra -Wpedantic)

if(RADAR_BUILD_TESTS)
  enable_testing()
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()
  include(GoogleTest)

  add_executable(unit_tests
    tests/unit/test_backoff.cpp
    tests/unit/test_bounded_queue.cpp
    tests/unit/test_config.cpp
    tests/unit/test_line_buffer.cpp
    tests/unit/test_liveness.cpp
    tests/unit/test_protocol.cpp
    tests/unit/test_track_frame.cpp
  )
  target_link_libraries(unit_tests PRIVATE radar_core GTest::gtest_main)
  set(RADAR_TEST_ENV "")
  if(RADAR_SANITIZER STREQUAL "thread")
    set(RADAR_TEST_ENV "TSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/tests/tsan.supp")
  endif()

  gtest_discover_tests(unit_tests PROPERTIES LABELS unit ENVIRONMENT "${RADAR_TEST_ENV}")

  add_executable(integration_tests
    tests/integration/test_reconnect.cpp
    tests/integration/test_server.cpp
  )
  target_include_directories(integration_tests PRIVATE tests/support)
  target_link_libraries(integration_tests PRIVATE radar_core GTest::gtest_main)
  target_compile_definitions(integration_tests PRIVATE RADAR_CLIENT_PATH="$<TARGET_FILE:radar_client>")
  add_dependencies(integration_tests radar_client)
  gtest_discover_tests(integration_tests PROPERTIES LABELS integration TIMEOUT 60 ENVIRONMENT "${RADAR_TEST_ENV}")
endif()
