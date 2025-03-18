cmake_minimum_required(VERSION 3.4)

project(pslite C CXX)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

# Enable testing
enable_testing()

# 系统检查
include(CheckIncludeFile)
include(CheckFunctionExists)
include(CheckSymbolExists)

# 网络相关检查
check_include_file("sys/socket.h" HAVE_SYS_SOCKET_H)
check_include_file("netinet/in.h" HAVE_NETINET_IN_H)
check_include_file("arpa/inet.h" HAVE_ARPA_INET_H)
check_function_exists("gethostbyname" HAVE_GETHOSTBYNAME)
check_function_exists("bind" HAVE_BIND)
check_function_exists("socket" HAVE_SOCKET)

if(NOT HAVE_SYS_SOCKET_H OR NOT HAVE_NETINET_IN_H OR NOT HAVE_ARPA_INET_H)
  message(WARNING "缺少关键网络头文件, 编译可能会失败")
endif()

add_library(pslite)

if(POLICY CMP0074)  # Support Protobuf_ROOT etc environment variables on CMake 3.12+
  cmake_policy(SET CMP0074 NEW)
endif()

# Find required dependencies
find_package(Threads REQUIRED)

# ---[ zmq
find_package(ZMQ)
if(NOT ZMQ_FOUND)
  cmake_minimum_required(VERSION 3.11)
  include(FetchContent)
  FetchContent_Declare(zmq
    URL https://github.com/zeromq/libzmq/releases/download/v4.3.4/zeromq-4.3.4.tar.gz
    URL_HASH SHA256=c593001a89f5a85dd2ddf564805deb860e02471171b3f204944e8a1b1f49bbe0)
  FetchContent_GetProperties(zmq)
  if(NOT zmq_POPULATED)
    FetchContent_Populate(zmq)
    if(POLICY CMP0077)  # Avoid building shared library and tests on CMake 3.13+
      cmake_policy(SET CMP0077 NEW)
      set(BUILD_SHARED OFF CACHE BOOL "")
      set(BUILD_TESTS OFF CACHE BOOL "")
    endif()
    add_subdirectory(${zmq_SOURCE_DIR} ${zmq_BINARY_DIR})
  endif()
  target_link_libraries(pslite PUBLIC libzmq-static)
else()
  target_include_directories(pslite PRIVATE ${ZMQ_INCLUDE_DIRS})
  target_link_libraries(pslite PUBLIC ${ZMQ_LIBRARIES})
endif()

# ---[ Google-protobuf
# Workaround broken DLAMI. DLAMI ships a broken protoc at /home/ubuntu/anaconda3/bin
# https://docs.aws.amazon.com/dlami/latest/devguide/overview-base.html
set(CMAKE_IGNORE_PATH "/home/ubuntu/anaconda3/bin")
set(PROTOBUF_GENERATE_CPP_APPEND_PATH TRUE)
find_package(Protobuf REQUIRED)
target_link_libraries(pslite PUBLIC ${Protobuf_LIBRARIES})
target_include_directories(pslite PUBLIC ${Protobuf_INCLUDE_DIRS})

# Add pthread library
target_link_libraries(pslite PUBLIC ${CMAKE_THREAD_LIBS_INIT})

# Generate protobuf headers and sources
file(GLOB_RECURSE PROTO_FILES "src/*.proto")
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
target_sources(pslite PRIVATE ${PROTO_SRCS})
target_include_directories(pslite PUBLIC ${CMAKE_CURRENT_BINARY_DIR})  # PROTO_HDRS files are here

# Generate protobuf python interface
if(Protobuf_PROTOC_EXECUTABLE)
  set(PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
else()
  set(PROTOC_EXECUTABLE ${PROTOBUF_PROTOC_EXECUTABLE})
endif()

if(PROTOC_EXECUTABLE)
  set(PROTO_PYS)
  foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_DIR ${PROTO_FILE} DIRECTORY)
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    set(PROTO_PY "${PROTO_DIR}/${PROTO_NAME}_pb2.py")
    list(APPEND PROTO_PYS ${PROTO_PY})
    add_custom_command(
      OUTPUT ${PROTO_PY}
      COMMAND ${PROTOC_EXECUTABLE} --python_out=${PROTO_DIR} --proto_path=${PROTO_DIR} ${PROTO_FILE}
      DEPENDS ${PROTO_FILE}
      COMMENT "Generating Python protobuf for ${PROTO_FILE}"
    )
  endforeach()
  add_custom_target(proto_python ALL DEPENDS ${PROTO_PYS})
  add_dependencies(pslite proto_python)
endif()

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc")
  FILE(GLOB getopt_SOURCE "src/windows/getopt.c")
  target_sources(pslite PRIVATE ${getopt_SOURCE})
  add_definitions(-DSTATIC_GETOPT)
  target_include_directories(pslite PRIVATE "${PROJECT_SOURCE_DIR}/src/windows")
  target_link_libraries(pslite PUBLIC "ipHlpApi.lib" "ws2_32.lib")
  foreach(flag_var
      CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE
      CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
    if(${flag_var} MATCHES "/MD")
      string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
    endif(${flag_var} MATCHES "/MD")
  endforeach(flag_var)
endif()

file(GLOB_RECURSE SOURCE "src/*.cc")
target_include_directories(pslite PUBLIC "${PROJECT_SOURCE_DIR}/include/")
target_sources(pslite PRIVATE ${SOURCE})

add_subdirectory(tests)
