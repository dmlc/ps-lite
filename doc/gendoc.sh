#!/bin/bash

dir=`dirname "$0"`
cd $dir/..
doxygen doc/Doxyfile
