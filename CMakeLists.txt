cmake_minimum_required(VERSION 3.1)

project(pslite C CXX)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

add_library(pslite)

# ---[ zmq
find_package(ZMQ)
if(NOT ZMQ_FOUND)
  cmake_minimum_required(VERSION 3.11)
  include(FetchContent)
  FetchContent_Declare(zmq
    URL https://github.com/zeromq/libzmq/releases/download/v4.3.2/zeromq-4.3.2.zip
    URL_HASH SHA256=d01517983ded9ff1b6c40f8206fc2b44ac96157a5aea7b974e8b0079547edfda)
  FetchContent_GetProperties(zmq)
  if(NOT zmq_POPULATED)
    FetchContent_Populate(zmq)
    if(POLICY CMP0077)
      # Avoid building shared library and tests on CMake 3.13+
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
include(cmake/ProtoBuf.cmake)
target_link_libraries(pslite PUBLIC ${PROTOBUF_LIBRARY})
target_include_directories(pslite PUBLIC ${PROTOBUF_INCLUDE_DIR})

FILE(GLOB SOURCE "src/*.cc")

# generate protobuf sources
set(proto_gen_folder "${PROJECT_BINARY_DIR}/src")
file(GLOB_RECURSE proto_files "src/*.proto")
pslite_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python "${PROJECT_SOURCE_DIR}" "src" ${proto_files})
list(APPEND SOURCE ${proto_srcs})

target_include_directories(pslite PUBLIC "${PROJECT_SOURCE_DIR}/include/")
target_include_directories(pslite PUBLIC "${PROJECT_BINARY_DIR}/include/")
target_include_directories(pslite PRIVATE "${PROJECT_BINARY_DIR}/src/")

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc")
  FILE(GLOB getopt_SOURCE "src/windows/getopt.c")
  list(APPEND SOURCE ${getopt_SOURCE})
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

target_sources(pslite PRIVATE ${SOURCE})
