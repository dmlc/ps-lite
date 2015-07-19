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

ifeq ($(STATIC_DEPS), 0)
PS_LDFLAGS += -L$(DEPS_PATH)/lib -lglog -lprotobuf -lgflags -lzmq -lcityhash -llz4
else
PS_LDFLAGS += $(addprefix $(DEPS_PATH)/lib/, libprotobuf.a libglog.a libgflags.a libzmq.a libcityhash.a liblz4.a) #-lgssapi_krb5
endif
