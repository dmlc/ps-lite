guide: $(addprefix guide/example_, a b c d e) #guide/network_perf # c d e


LDFLAGS = $(PS_LDFLAGS) -lpthread $(EXTRA_LDFLAGS)

OS := $(shell uname -s)
ifneq ($(OS), Darwin)
    LDFLAGS += -lrt
endif

guide/%: guide/%.cc $(PS_LIB) $(PS_MAIN)
	$(CXX) $(CFLAGS) -fopenmp $^ $(LDFLAGS) -o $@
