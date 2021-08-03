#!/bin/bash

bin=clang-format

set -ex

find src -name '*.cc' -o -name '*.h' ! -name 'ucx_van.h' | xargs $bin -i -style=file
find include -name '*.h' ! -name spsc_queue.h ! -name logging.h ! -name base.h ! -name parallel_*.h | xargs $bin -i -style=file

echo "format code done"
