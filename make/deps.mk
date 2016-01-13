# Install dependencies

URL=https://raw.githubusercontent.com/mli/deps/master/build

# protobuf

PROTOBUF = ${DEPS_PATH}/include/google/protobuf/message.h

${PROTOBUF}:
	$(eval FILE=protobuf-2.5.0.tar.gz)
	$(eval DIR=protobuf-2.5.0)
	rm -rf $(FILE) $(DIR)
	wget $(URL)/$(FILE) && tar -zxf $(FILE)
	cd $(DIR) && export CFLAGS=-fPIC && export CXXFLAGS=-fPIC && ./configure -prefix=$(DEPS_PATH) && $(MAKE) && $(MAKE) install
	rm -rf $(FILE) $(DIR)

# protobuf: | ${PROTOBUF}

# zmq

ZMQ = ${DEPS_PATH}/include/zmq.h

${ZMQ}:
	$(eval FILE=zeromq-4.1.2.tar.gz)
	$(eval DIR=zeromq-4.1.2)
	rm -rf $(FILE) $(DIR)
	wget $(URL)/$(FILE) && tar -zxf $(FILE)
	cd $(DIR) && export CFLAGS=-fPIC && export CXXFLAGS=-fPIC && ./configure -prefix=$(DEPS_PATH) --with-libsodium=no --with-libgssapi_krb5=no && $(MAKE) && $(MAKE) install
	rm -rf $(FILE) $(DIR)

# zmq: | ${ZMQ}

# # gflags

# ${DEPS_PATH}/include/google/gflags.h:
# 	$(eval FILE=gflags-2.0-no-svn-files.tar.gz)
# 	$(eval DIR=gflags-2.0)
# 	rm -rf $(FILE) $(DIR)
# 	wget $(URL)/$(FILE) && tar -zxf $(FILE)
# 	cd $(DIR) && ./configure -prefix=$(DEPS_PATH) && $(MAKE) && $(MAKE) install
# 	rm -rf $(FILE) $(DIR)

# gflags: | ${DEPS_PATH}/include/google/gflags.h

# # glog

# ${DEPS_PATH}/include/glog/logging.h: | ${DEPS_PATH}/include/google/gflags.h
# 	$(eval FILE=v0.3.4.tar.gz)
# 	$(eval DIR=glog-0.3.4)
# 	rm -rf $(FILE) $(DIR)
# 	wget https://github.com/google/glog/archive/$(FILE) && tar -zxf $(FILE)
# 	cd $(DIR) && ./configure -prefix=$(DEPS_PATH) --with-gflags=$(DEPS_PATH) && $(MAKE) && $(MAKE) install
# 	rm -rf $(FILE) $(DIR)

# glog: | ${DEPS_PATH}/include/glog/logging.h
# # lz4

# ${DEPS_PATH}/include/lz4.h:
# 	$(eval FILE=lz4-r129.tar.gz)
# 	$(eval DIR=lz4-r129)
# 	rm -rf $(FILE) $(DIR)
# 	wget $(URL)/$(FILE) && tar -zxf $(FILE)
# 	cd $(DIR) && $(MAKE) && PREFIX=$(DEPS_PATH) $(MAKE) install
# 	rm -rf $(FILE) $(DIR)

# lz4: | ${DEPS_PATH}/include/lz4.h

# # cityhash

# ${DEPS_PATH}/include/city.h:
# 	$(eval FILE=cityhash-1.1.1.tar.gz)
# 	$(eval DIR=cityhash-1.1.1)
# 	rm -rf $(FILE) $(DIR)
# 	wget $(URL)/$(FILE) && tar -zxf $(FILE)
# 	cd $(DIR) && ./configure -prefix=$(DEPS_PATH) --enable-sse4.2 && $(MAKE) CXXFLAGS="-g -O3 -msse4.2" && $(MAKE) install
# 	rm -rf $(FILE) $(DIR)

# cityhash: | ${DEPS_PATH}/include/city.h
