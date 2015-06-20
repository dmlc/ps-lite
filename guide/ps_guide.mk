guide: $(addprefix guide/example_, a b c d e) guide/network_perf

LDFLAGS = $(EXTRA_LDFLAGS) $(PS_LDFLAGS) $(CORE_PATH)/libdmlc.a -lpthread # -lrt

guide/%: guide/%.cc $(PS_LIB) $(PS_MAIN)
	$(CXX) $(CFLAGS) $^ $(LDFLAGS) -o $@
