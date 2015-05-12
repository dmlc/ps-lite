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

ifeq ($(STATIC_DEPS), 1)
PS_LDFLAGS += $(addprefix $(DEPS_PATH)/lib/, libgflags.a libprotobuf.a libglog.a libzmq.a libcityhash.a)
else
PS_LDFLAGS += -L$(DEPS_PATH)/lib -lgflags -lprotobuf -lglog -lzmq -lcityhash
endif

USE_GLOG = 1
