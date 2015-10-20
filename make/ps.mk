#---------------------------------------------------------------------------------------
#  parameter server configuration script
#
#  include ps.mk after the variables are set
#
#----------------------------------------------------------------------------------------

ifeq ($(USE_KEY32), 1)
ADD_CFLAGS += -DUSE_KEY32=1
endif

PS_LDFLAGS_SO = -L$(DEPS_PATH)/lib -lglog -lprotobuf -lzmq
PS_LDFLAGS_A = $(addprefix $(DEPS_PATH)/lib/, libprotobuf.a libglog.a libzmq.a)

# ifeq ($(STATIC_DEPS), 0)
# ADD_LDFLAGS += $(PS_LDFLAGS_SO)
# else
# ADD_LDFLAGS += $(PS_LDFLAGS_A)
# endif
