#!/bin/bash
#
# in default, it will install all deps in $ROOT/deps, you can change the
# install_dir
#
#

install_dir=$PWD/`dirname $0`/../deps
mkdir -p $install_dir
cd $install_dir

rm -f install.sh
wget -q https://raw.githubusercontent.com/mli/deps/master/install.sh
source ./install.sh

install_gflags
install_protobuf
install_glog
install_zmq
install_lz4
install_cityhash
