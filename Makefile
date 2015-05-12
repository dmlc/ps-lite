ifneq ("$(wildcard ./config.mk)","")
include ./config.mk
else
include make/config.mk
endif

include make/ps.mk

WARN = -Wall -finline-functions
INCPATH = -I./src -I./include -I$(DEPS_PATH)/include
CFLAGS = -std=c++11 -msse2 $(WARN) $(OPT) $(INCPATH) $(PS_CFLAGS) $(EXTRA_CFLAGS)


PS_LIB = build/libps.a
PS_MAIN = build/libps_main.a
# TEST_MAIN = build/test_main.o

clean:
	rm -rf build
	find src -name "*.pb.[ch]*" -delete

ps: $(PS_LIB) $(PS_MAIN) #$(TEST_MAIN)

# PS system
ps_srcs	= $(wildcard src/*.cc src/*/*.cc)
ps_protos	= $(wildcard src/proto/*.proto)
ps_objs	= $(patsubst src/%.proto, build/%.pb.o, $(ps_protos)) \
			  $(patsubst src/%.cc, build/%.o, $(ps_srcs))

build/libps.a: $(patsubst %.proto, %.pb.h, $(ps_protos)) $(ps_objs)
	ar crv $@ $(filter %.o, $?)

build/libps_main.a: build/ps_main.o
	ar crv $@ $?

build/%.o: src/%.cc
	@mkdir -p $(@D)
	$(CC) $(INCPATH) -std=c++0x -MM -MT build/$*.o $< >build/$*.d
	$(CC) $(CFLAGS) -c $< -o $@

%.pb.cc %.pb.h : %.proto
	${DEPS_PATH}/bin/protoc --cpp_out=./src --proto_path=./src $<

-include build/*/*.d
-include build/*/*/*.d
-include test/ps_test.mk
-include guide/ps_guide.mk
