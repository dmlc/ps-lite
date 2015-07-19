#!/bin/bash

# main script of travis
if [ ${TASK} == "lint" ]; then
    make lint || exit -1
fi

if [ ${TASK} == "doc" ]; then
    make doc 2>log.txt
    (cat log.txt| grep -v ENABLE_PREPROCESSING |grep -v "unsupported tag" |grep warning) && exit -1
fi

if [ ${TASK} == "build" ]; then
    make DEPS_PATH=${CACHE_PREFIX} CXX=${CXX} || exit -1
fi

if [ ${TASK} == "unittest_gtest" ]; then
    cp make/config.mk .
    echo "BUILD_TEST=1" >> config.mk
    make all || exit -1
    test/unittest/dmlc_unittest || exit -1
fi
