ifdef config
include $(config)
endif

include make/ps.mk

ifndef CXX
CXX = g++
endif

ifndef DEPS_PATH
DEPS_PATH = $(shell pwd)/deps
endif

ifndef FABRIC_PATH
FABRIC_PATH = /opt/amazon/efa
endif

ifndef PROTOC
PROTOC = ${DEPS_PATH}/bin/protoc
endif


INCPATH = -I./src -I./include -I$(DEPS_PATH)/include
CFLAGS = -std=c++14 -msse2 -fPIC -O3 -ggdb -Wall -finline-functions $(INCPATH) $(ADD_CFLAGS)
LIBS = -pthread -lrt

ifeq ($(USE_CUDA), 1)
LIBS += -lcudart -L$(CUDA_HOME)/lib64
CFLAGS += -DDMLC_USE_CUDA
INCPATH += -I$(CUDA_HOME)/include
endif

ifeq ($(USE_RDMA), 1)
LIBS += -lrdmacm -libverbs
CFLAGS += -DDMLC_USE_RDMA
endif

ifeq ($(USE_FABRIC), 1)
LIBS += -lfabric -L$(FABRIC_PATH)/lib64 -L$(FABRIC_PATH)/lib
CFLAGS += -DDMLC_USE_FABRIC
INCPATH += -I$(FABRIC_PATH)/include
endif

ifeq ($(USE_UCX), 1)
LIBS += -lucp -luct -lucs -lucm
CFLAGS += -DDMLC_USE_UCX
	ifdef UCX_PATH
	LIBS += -L$(UCX_PATH)/lib
	INCPATH += -I$(UCX_PATH)/include
	endif
endif

ifdef ASAN
CFLAGS += -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls
endif


all: ps test

include make/deps.mk

clean:
	rm -rf build $(TEST) tests/*.d tests/*.dSYM

lint:
	python tests/lint.py ps all include/ps src

ps: build/libps.a

OBJS = $(addprefix build/, customer.o postoffice.o van.o)
build/libps.a: $(OBJS)
	ar crv $@ $(filter %.o, $?)

build/%.o: src/%.cc ${ZMQ}
	@mkdir -p $(@D)
	$(CXX) $(CFLAGS) $(INCPATH) -MM -MT build/$*.o $< >build/$*.d
	$(CXX) $(CFLAGS) $(LIBS) -c $< -o $@

-include build/*.d
-include build/*/*.d

include tests/test.mk
test: $(TEST)
