# glog depends on gflags
include("cmake/External/gflags.cmake")

set(GFLAGS_ROOT_DIR ${gflags_INSTALL})

if (NOT __GLOG_INCLUDED)
  set(__GLOG_INCLUDED TRUE)

  # try the system-wide glog first
  find_package(Glog)
  if (GLOG_FOUND)
      set(GLOG_EXTERNAL FALSE)
  else()
    # fetch and build glog from github

    # build directory
    set(glog_PREFIX ${CMAKE_BINARY_DIR}/external/glog-prefix)
    # install directory
    set(glog_INSTALL ${CMAKE_BINARY_DIR}/external/glog-install)

    # we build glog statically, but want to link it into the caffe shared library
    # this requires position-independent code
    if (UNIX)
      set(GLOG_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(GLOG_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${GLOG_EXTRA_COMPILER_FLAGS})
    set(GLOG_C_FLAGS ${CMAKE_C_FLAGS} ${GLOG_EXTRA_COMPILER_FLAGS})

    # depend on gflags if we're also building it
    if (GFLAGS_EXTERNAL)
      set(GLOG_DEPENDS gflags)
    endif()

	
    ExternalProject_Add(glog
      DEPENDS ${GLOG_DEPENDS}
      PREFIX ${glog_PREFIX}
      GIT_REPOSITORY "https://github.com/google/glog"
      UPDATE_COMMAND ""
      INSTALL_DIR ${glog_INSTALL}
	  CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
           -DCMAKE_INSTALL_PREFIX=${glog_INSTALL}
		   -DCMAKE_PREFIX_PATH=${gflags_INSTALL}
		   -Dgflags_FOUND=ON
		   -Dgflags_INCLUDE_DIR=${gflags_INSTALL}/Include
		   -Dgflags_INCLUDE_DIR=${gflags_INSTALL}/Lib/gflags
		   -DBUILD_SHARED_LIBS=OFF
           -DINSTALL_HEADERS=ON
           -DCMAKE_C_FLAGS=${GLOG_C_FLAGS}
           -DCMAKE_CXX_FLAGS=${GLOG_CXX_FLAGS}
      LOG_DOWNLOAD 1
      LOG_CONFIGURE 1
      LOG_INSTALL 1
      )

    set(GLOG_FOUND TRUE)
    set(GLOG_INCLUDE_DIRS ${glog_INSTALL}/include)
	if(MSVC)
		set(GLOG_LIBRARIES ${GFLAGS_LIBRARIES} ${glog_INSTALL}/lib/glog.lib)
	else()
		set(GLOG_LIBRARIES ${GFLAGS_LIBRARIES} ${glog_INSTALL}/lib/libglog.a)
	endif()
    set(GLOG_LIBRARY_DIRS ${glog_INSTALL}/lib)
    set(GLOG_EXTERNAL TRUE)

    list(APPEND external_project_dependencies glog)
  endif()

endif()

