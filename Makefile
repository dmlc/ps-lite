ifdef config
include $(config)
endif

include make/ps.mk

ifndef CXX
CXX = g++
endif

ifndef OPT
OPT  =
endif

ifndef DEPS_PATH
DEPS_PATH = $(shell pwd)/deps
endif

ifndef PROTOC
PROTOC = ${DEPS_PATH}/bin/protoc
endif

INCPATH = -I./src -I./include -I$(DEPS_PATH)/include
CFLAGS = -std=c++11 -msse2 -fPIC -O0 -ggdb -Wall -finline-functions $(INCPATH) $(ADD_CFLAGS)

PS_LIB = build/libps.a
# PS_MAIN = build/libps_main.a


all: deps ps test #guide

clean:
	rm -rf build $(TEST) tests/*.d
	find include -name "*.pb.cc" -delete

ps: $(PS_LIB) $(PS_MAIN)

ps_srcs	= $(wildcard src/*.cc src/*/*.cc)
ps_protos	= $(wildcard include/ps/internal/*.proto)
ps_objs	= $(patsubst include/ps/internal/%.proto, build/%.pb.o, $(ps_protos)) \
			  $(patsubst src/%.cc, build/%.o, $(ps_srcs))

build/libps.a: $(patsubst %.proto, %.pb.h, $(ps_protos)) $(ps_objs)
	ar crv $@ $(filter %.o, $?)

# build/libps_main.a: build/ps_main.o
# 	ar crv $@ $?

build/%.o: src/%.cc
	@mkdir -p $(@D)
	$(CXX) $(INCPATH) -std=c++0x -MM -MT build/$*.o $< >build/$*.d
	$(CXX) $(CFLAGS) -c $< -o $@

build/%.o: include/ps/internal/%.cc
	@mkdir -p build
	$(CXX) $(CFLAGS) -c $< -o $@

%.pb.cc %.pb.h : %.proto
	$(PROTOC) --cpp_out=./include --proto_path=./include $<

-include build/*.d
-include build/*/*.d
# -include guide/ps_guide.mk

# deps
include make/deps.mk

deps: glog protobuf zmq

# test

-include tests/test.mk

test: $(TEST)
	echo $(TEST)
