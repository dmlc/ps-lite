#!/bin/bash
# main script of travis

if [ ${TASK} == "lint" ]; then
    make lint || exit -1
fi

if [ ${TASK} == "build" ]; then
    make DEPS_PATH=${CACHE_PREFIX} CXX=${CXX} || exit -1
fi
