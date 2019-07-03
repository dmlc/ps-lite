TEST_SRC = $(wildcard tests/test_*.cc)
TEST = $(patsubst tests/test_%.cc, tests/test_%, $(TEST_SRC))

# -ltcmalloc_and_profiler
LDFLAGS = -Wl,-rpath,$(DEPS_PATH)/lib $(PS_LDFLAGS_SO) -pthread
tests/% : tests/%.cc build/libps.a
	$(CXX) -std=c++0x $(CFLAGS) -MM -MT tests/$* $< >tests/$*.d $(LIBS)
	$(CXX) -std=c++0x $(CFLAGS) -o $@ $(filter %.cc %.a, $^) $(LDFLAGS) $(LIBS)

-include tests/*.d
