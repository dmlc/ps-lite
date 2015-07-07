guide: $(addprefix guide/example_, a b c) guide/network_perf # d e

LDFLAGS = $(EXTRA_LDFLAGS) $(PS_LDFLAGS) -lpthread # -lrt

guide/%: guide/%.cc $(PS_LIB) $(PS_MAIN)
	$(CXX) $(CFLAGS) $^ $(LDFLAGS) -o $@
