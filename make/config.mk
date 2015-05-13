# default configuration of make
#
# you can copy it to the parent directory and modify it as you want. then
# compile by `make -j 8` using 8 threads

# compiler
CXX = g++

# optimization flag. -O0 -ggdb for debug
# OPT = -O3 -ggdb

# statically link all dependent libraries, such as gflags, zeromq, if
# 1. otherwise use dynamic linking
# STATIC_DEPS = 0

# the installed path of third party libraries
# DEPS_PATH = $(shell pwd)/deps

# additional link flags, such as -ltcmalloc_and_profiler
# EXTRA_LDFLAGS =

# additional compile flags
# EXTRA_CFLAGS =
