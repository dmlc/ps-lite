cmake_minimum_required(VERSION 2.8.7)

project(pslite C CXX)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

include(ExternalProject) 
set(pslite_LINKER_LIBS "")

# ---[ Google-gflags
include("cmake/External/gflags.cmake")
include_directories(pslite ${GFLAGS_INCLUDE_DIRS})
list(APPEND pslite_LINKER_LIBS ${GFLAGS_LIBRARIES})

# ---[ Google-glog
include("cmake/External/glog.cmake")
include_directories(pslite ${GLOG_INCLUDE_DIRS})
list(APPEND pslite_LINKER_LIBS ${GLOG_LIBRARIES})

# ---[ Google-protobuf
include(cmake/ProtoBuf.cmake)

# generate protobuf sources
set(proto_gen_folder "${PROJECT_BINARY_DIR}/include")
file(GLOB proto_files "src/proto/*.proto")
pslite_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python "${PROJECT_SOURCE_DIR}" "src" ${proto_files})
include_directories(pslite "${PROJECT_BINARY_DIR}/include/")

FILE(GLOB SOURCE "src/*.cc")

if(MSVC)
FILE(GLOB getopt_SOURCE "src/windows/getopt.c")
list(APPEND SOURCE ${getopt_SOURCE}) 
add_definitions(-DSTATIC_GETOPT)
endif()

list(APPEND SOURCE ${proto_srcs}) 
add_library(pslite ${SOURCE}) 

target_link_libraries(pslite ${pslite_LINKER_LIBS})