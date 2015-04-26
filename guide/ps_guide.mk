guide: \
guide/example_a guide/example_b guide/example_c guide/example_d

guide/%: guide/%.cc $(PS_LIB)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@
