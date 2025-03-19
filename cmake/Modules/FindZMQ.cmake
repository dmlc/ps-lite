# FindZMQ.cmake
# ZMQ
#
# 使用此模块可以查找ZMQ库
# 此模块将定义以下变量:
#  ZMQ_FOUND - 系统中是否有ZMQ
#  ZMQ_INCLUDE_DIRS - ZMQ包含目录
#  ZMQ_LIBRARIES - ZMQ库文件
#  ZMQ_VERSION - ZMQ版本

# 首先尝试deps目录
find_path(ZMQ_INCLUDE_DIR zmq.h
  PATHS ${CMAKE_SOURCE_DIR}/deps/include
  NO_DEFAULT_PATH
)

find_library(ZMQ_LIBRARY
  NAMES zmq libzmq
  PATHS ${CMAKE_SOURCE_DIR}/deps/lib
  NO_DEFAULT_PATH
)

# 如果deps目录未找到，则尝试系统目录
if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  find_path(ZMQ_INCLUDE_DIR zmq.h)
  find_library(ZMQ_LIBRARY NAMES zmq libzmq)
endif()

if(ZMQ_INCLUDE_DIR AND ZMQ_LIBRARY)
  set(ZMQ_FOUND TRUE)
  set(ZMQ_LIBRARIES ${ZMQ_LIBRARY})
  set(ZMQ_INCLUDE_DIRS ${ZMQ_INCLUDE_DIR})
  
  # 提取版本
  if(EXISTS "${ZMQ_INCLUDE_DIR}/zmq.h")
    file(STRINGS "${ZMQ_INCLUDE_DIR}/zmq.h" ZMQ_VERSION_MAJOR_LINE REGEX "^#define[ \t]+ZMQ_VERSION_MAJOR[ \t]+[0-9]+$")
    file(STRINGS "${ZMQ_INCLUDE_DIR}/zmq.h" ZMQ_VERSION_MINOR_LINE REGEX "^#define[ \t]+ZMQ_VERSION_MINOR[ \t]+[0-9]+$")
    file(STRINGS "${ZMQ_INCLUDE_DIR}/zmq.h" ZMQ_VERSION_PATCH_LINE REGEX "^#define[ \t]+ZMQ_VERSION_PATCH[ \t]+[0-9]+$")
    
    string(REGEX REPLACE "^#define[ \t]+ZMQ_VERSION_MAJOR[ \t]+([0-9]+)$" "\\1" ZMQ_VERSION_MAJOR "${ZMQ_VERSION_MAJOR_LINE}")
    string(REGEX REPLACE "^#define[ \t]+ZMQ_VERSION_MINOR[ \t]+([0-9]+)$" "\\1" ZMQ_VERSION_MINOR "${ZMQ_VERSION_MINOR_LINE}")
    string(REGEX REPLACE "^#define[ \t]+ZMQ_VERSION_PATCH[ \t]+([0-9]+)$" "\\1" ZMQ_VERSION_PATCH "${ZMQ_VERSION_PATCH_LINE}")
    
    set(ZMQ_VERSION "${ZMQ_VERSION_MAJOR}.${ZMQ_VERSION_MINOR}.${ZMQ_VERSION_PATCH}")
  endif()

else()
  set(ZMQ_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZMQ 
  REQUIRED_VARS ZMQ_LIBRARY ZMQ_INCLUDE_DIR
  VERSION_VAR ZMQ_VERSION
)

mark_as_advanced(ZMQ_INCLUDE_DIR ZMQ_LIBRARY) 