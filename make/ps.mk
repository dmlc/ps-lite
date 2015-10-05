#---------------------------------------------------------------------------------------
#  parameter server configuration script
#
#  include ps.mk after the variables are set
#
#  Add PS_CFLAGS to the compile flags
#  Add PS_LDFLAGS to the linker flags
#----------------------------------------------------------------------------------------

ifeq ($(USE_KEY32), 1)
PS_CFLAGS += -DUSE_KEY32=1
endif
PS_LDFLAGS_SO = -L$(DEPS_PATH)/lib -lglog -lprotobuf -lgflags -lzmq -lcityhash -llz4
PS_LDFLAGS_A = $(addprefix $(DEPS_PATH)/lib/, libprotobuf.a libglog.a libgflags.a libzmq.a libcityhash.a liblz4.a)

ifeq ($(STATIC_DEPS), 0)
PS_LDFLAGS += $(PS_LDFLAGS_SO)
else
PS_LDFLAGS += $(PS_LDFLAGS_A)
endif
