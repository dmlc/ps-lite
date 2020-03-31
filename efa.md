## Build with EFA

AMI: Base DLAMI (ubuntu/AML)

for AML install gcc-4.9 first

```
set -e

wget https://ftp.gnu.org/gnu/gcc/gcc-4.9.3/gcc-4.9.3.tar.gz
tar xzf gcc-4.9.3.tar.gz
cd gcc-4.9.3
./contrib/download_prerequisites
./configure --disable-multilib --enable-languages=c,c++
make -j$(nproc)
sudo make install
```

```
make clean; USE_FABRIC=1 make -j;

killall test_benchmark; make clean; make -j USE_FABRIC=1;

```

## Run Test in 3 windows
```
DMLC_ENABLE_RDMA=lffabric PS_VERBOSE=2 NUM_KEY_PER_SERVER=40 DMLC_ROLE=worker bash tests/local_multi_workers.sh 1 1 tests/test_benchmark 1024000 100 1
DMLC_ENABLE_RDMA=lffabric PS_VERBOSE=2 DMLC_ROLE=scheduler bash tests/local_multi_workers.sh 1 1 tests/test_benchmark
DMLC_ENABLE_RDMA=lffabric PS_VERBOSE=2 DMLC_ROLE=server    bash tests/local_multi_workers.sh 1 1 tests/test_benchmark

```
