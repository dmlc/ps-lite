guide: $(addprefix guide/example_, a b c d e) #guide/network_perf # c d e

LDFLAGS = $(EXTRA_LDFLAGS) $(PS_LDFLAGS) -lpthread # -lrt

guide/%: guide/%.cc $(PS_LIB) $(PS_MAIN)
	$(CXX) $(CFLAGS) $^ $(LDFLAGS) -o $@
