cmake_minimum_required(VERSION 3.1)

project(pslite C CXX)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

add_library(pslite)

# ---[ zmq
include("cmake/External/zmq.cmake")
target_include_directories(pslite PRIVATE ${ZMQ_INCLUDE_DIRS})
target_link_libraries(pslite PUBLIC ${ZMQ_LIBRARIES})
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
