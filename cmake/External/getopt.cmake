if (NOT __GETOPT_INCLUDED) # guard against multiple includes
  set(__GETOPT_INCLUDED TRUE)
  include(CheckIncludeFile)
  check_include_file("getopt.h" HAVE_GETOPT_H)
  if(NOT HAVE_GETOPT_H)
    find_path(GETOPT_INCLUDE_DIRS "getopt.h")
    find_library(GETOPT_LIBRARIES "getopt")
  endif()
  
endif()
