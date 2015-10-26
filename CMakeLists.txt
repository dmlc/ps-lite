cmake_minimum_required(VERSION 2.8.7)

project(pslite C CXX)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

include(ExternalProject) 
set(pslite_LINKER_LIBS "")

# ---[ zmq
include("cmake/External/zmq.cmake")
include_directories(pslite ${ZMQ_INCLUDE_DIRS})
list(APPEND pslite_LINKER_LIBS ${ZMQ_LIBRARIES})

# ---[ Google-protobuf
include(cmake/ProtoBuf.cmake)

# generate protobuf sources
set(proto_gen_folder "${PROJECT_BINARY_DIR}/include")
file(GLOB_RECURSE proto_files "include/*.proto")
pslite_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python "${PROJECT_SOURCE_DIR}" "include" ${proto_files})
include_directories(pslite "${PROJECT_SOURCE_DIR}/include/")
include_directories(pslite "${PROJECT_BINARY_DIR}/include/")

FILE(GLOB SOURCE "src/*.cc")

if(MSVC)
FILE(GLOB getopt_SOURCE "src/windows/getopt.c")
list(APPEND SOURCE ${getopt_SOURCE}) 
add_definitions(-DSTATIC_GETOPT)
include_directories(pslite "${PROJECT_SOURCE_DIR}/src/windows")
endif()

list(APPEND SOURCE ${proto_srcs}) 
add_library(pslite ${SOURCE}) 

target_link_libraries(pslite ${pslite_LINKER_LIBS})