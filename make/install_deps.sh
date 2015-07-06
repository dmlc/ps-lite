#!/bin/bash
# usage: install_deps.sh install_dir
#
# if install_dir is not specified, it will install all deps in $ROOT/deps,
#

if [ $# -ne 1 ]; then
    install_dir=$PWD/`dirname $0`/../deps
else
    install_dir=$1
fi

mkdir -p $install_dir
cd $install_dir

rm -f install.sh
wget -q https://raw.githubusercontent.com/mli/deps/master/install.sh
source ./install.sh

if [ ! -f include/google/gflags.h ]; then
    install_gflags
else
    echo "skip gflags"
fi

if [ ! -f include/google/protobuf/message.h ]; then
    install_protobuf
else
    echo "skip protobuf"
fi

if [ ! -f include/glog/logging.h ]; then
    install_glog
else
    echo "skip glog"
fi

if [ ! -f include/zmq.h ]; then
    install_zmq
else
    echo "skip zmq"
fi

if [ ! -f include/city.h ]; then
    install_cityhash
else
    echo "skip cityhash"
fi

if [ ! -f include/lz4.h ]; then
    install_lz4
else
    echo "skip lz4"
fi


rm -rf $install_dir/build
