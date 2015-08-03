#!/bin/bash

doxygen

# put doxygen doc online

rm html.tar.gz
tar -zcf html.tar.gz html

host=linux.gp.cs.cmu.edu
scp html.tar.gz $host:~

ssh $host 'bash -s' <<EOF
rm -rf www/ps-lite html
tar -zxf html.tar.gz
mkdir www/ps-lite
cp -r html/* www/ps-lite
EOF
