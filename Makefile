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
CFLAGS = -std=c++11 -msse2 -fPIC -O3 -ggdb -Wall -finline-functions $(INCPATH) $(ADD_CFLAGS)

all: ps test #guide

# deps
include make/deps.mk

clean:
	rm -rf build $(TEST) tests/*.d
	find include -name "*.pb.[ch]*" -delete

ps: build/libps.a

OBJS = $(addprefix build/, customer.o postoffice.o van.o meta.pb.o)
build/libps.a: $(OBJS)
	ar crv $@ $(filter %.o, $?)

build/%.o: src/%.cc ${ZMQ}
	@mkdir -p $(@D)
	$(CXX) $(INCPATH) -std=c++0x -MM -MT build/$*.o $< >build/$*.d
	$(CXX) $(CFLAGS) -c $< -o $@

src/%.pb.cc src/%.pb.h : src/%.proto ${PROTOBUF}
	$(PROTOC) --cpp_out=./src --proto_path=./src $<

-include build/*.d
-include build/*/*.d
# -include guide/ps_guide.mk


# test
-include tests/test.mk
test: $(TEST)
