guide: $(addprefix guide/example_, a b c d)

LDFLAGS = $(EXTRA_LDFLAGS) $(PS_LDFLAGS) $(CORE_PATH)/libdmlc.a -lpthread # -lrt

guide/%: guide/%.cc $(PS_LIB)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@
