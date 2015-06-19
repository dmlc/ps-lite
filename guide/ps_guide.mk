guide: guide/example_a guide/example_b guide/example_c guide/example_d guide/example_e

LDFLAGS = $(EXTRA_LDFLAGS) $(PS_LDFLAGS) $(CORE_PATH)/libdmlc.a -lpthread # -lrt

guide/%: guide/%.cc $(PS_LIB)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@
