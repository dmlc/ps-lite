# FindProtobuf.cmake
# Protobuf
#
# 使用此模块可以查找Protobuf库
# 此模块将定义以下变量:
#  Protobuf_FOUND - 系统中是否有Protobuf
#  Protobuf_INCLUDE_DIRS - Protobuf包含目录
#  Protobuf_LIBRARIES - Protobuf库文件
#  Protobuf_PROTOC_EXECUTABLE - protoc可执行文件路径
#  Protobuf_VERSION - Protobuf版本

# 首先尝试deps目录
find_path(Protobuf_INCLUDE_DIR google/protobuf/message.h
  PATHS ${CMAKE_SOURCE_DIR}/deps/include
  NO_DEFAULT_PATH
)

find_library(Protobuf_LIBRARY
  NAMES protobuf libprotobuf
  PATHS ${CMAKE_SOURCE_DIR}/deps/lib
  NO_DEFAULT_PATH
)

find_program(Protobuf_PROTOC_EXECUTABLE
  NAMES protoc
  PATHS ${CMAKE_SOURCE_DIR}/deps/bin
  NO_DEFAULT_PATH
)

# 如果deps目录未找到，则尝试系统目录
if(NOT Protobuf_INCLUDE_DIR OR NOT Protobuf_LIBRARY OR NOT Protobuf_PROTOC_EXECUTABLE)
  find_path(Protobuf_INCLUDE_DIR google/protobuf/message.h)
  find_library(Protobuf_LIBRARY NAMES protobuf libprotobuf)
  find_program(Protobuf_PROTOC_EXECUTABLE NAMES protoc)
endif()

if(Protobuf_INCLUDE_DIR AND Protobuf_LIBRARY)
  set(Protobuf_FOUND TRUE)
  set(Protobuf_LIBRARIES ${Protobuf_LIBRARY})
  set(Protobuf_INCLUDE_DIRS ${Protobuf_INCLUDE_DIR})

  # 提取版本
  if(EXISTS "${Protobuf_INCLUDE_DIR}/google/protobuf/stubs/common.h")
    file(STRINGS "${Protobuf_INCLUDE_DIR}/google/protobuf/stubs/common.h" Protobuf_VERSION_LINE
         REGEX "^#define[ \t]+GOOGLE_PROTOBUF_VERSION[ \t]+[0-9]+$")
    
    if(Protobuf_VERSION_LINE)
      string(REGEX REPLACE "^#define[ \t]+GOOGLE_PROTOBUF_VERSION[ \t]+([0-9]+)$" "\\1" Protobuf_VERSION_NUM "${Protobuf_VERSION_LINE}")
      
      math(EXPR Protobuf_MAJOR "${Protobuf_VERSION_NUM} / 1000000")
      math(EXPR Protobuf_MINOR "${Protobuf_VERSION_NUM} / 1000 % 1000")
      math(EXPR Protobuf_PATCH "${Protobuf_VERSION_NUM} % 1000")
      
      set(Protobuf_VERSION "${Protobuf_MAJOR}.${Protobuf_MINOR}.${Protobuf_PATCH}")
    endif()
  endif()

else()
  set(Protobuf_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Protobuf
  REQUIRED_VARS Protobuf_LIBRARY Protobuf_INCLUDE_DIR Protobuf_PROTOC_EXECUTABLE
  VERSION_VAR Protobuf_VERSION
)

mark_as_advanced(Protobuf_INCLUDE_DIR Protobuf_LIBRARY Protobuf_PROTOC_EXECUTABLE)

# 设置生成函数
function(PROTOBUF_GENERATE_CPP SRCS HDRS)
  if(NOT ARGN)
    message(SEND_ERROR "Error: PROTOBUF_GENERATE_CPP() called without any proto files")
    return()
  endif()

  if(PROTOBUF_GENERATE_CPP_APPEND_PATH)
    set(APPEND_PATH TRUE)
  else()
    set(APPEND_PATH FALSE)
  endif()

  set(${SRCS})
  set(${HDRS})
  
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)
    get_filename_component(FIL_DIR ${ABS_FIL} DIRECTORY)
    
    set(HEADER_FILE "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")
    set(SRC_FILE "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
    
    list(APPEND ${SRCS} "${SRC_FILE}")
    list(APPEND ${HDRS} "${HEADER_FILE}")
    
    add_custom_command(
      OUTPUT "${SRC_FILE}" "${HEADER_FILE}"
      COMMAND ${Protobuf_PROTOC_EXECUTABLE}
      ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR} --proto_path=${FIL_DIR} ${ABS_FIL}
      DEPENDS ${ABS_FIL} ${Protobuf_PROTOC_EXECUTABLE}
      COMMENT "Running C++ protocol buffer compiler on ${FIL}"
      VERBATIM
    )
  endforeach()
  
  set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
  set(${SRCS} ${${SRCS}} PARENT_SCOPE)
  set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction() 