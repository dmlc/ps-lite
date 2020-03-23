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

DMLC_ENABLE_FABRIC=1 PS_VERBOSE=2 NUM_KEY_PER_SERVER=2 BYTEPS_USE_VEC=1 DMLC_ROLE=scheduler DMLC_EFA_DEBUG=1 ENABLE_RDMA_LOG=1 bash tests/local_multi_workers.sh 1 1 tests/test_benchmark 1024000 5 0

DMLC_ENABLE_FABRIC=1 PS_VERBOSE=2 NUM_KEY_PER_SERVER=2 BYTEPS_USE_VEC=1 DMLC_ROLE=server DMLC_EFA_DEBUG=1 ENABLE_RDMA_LOG=1 bash tests/local_multi_workers.sh 1 1 tests/test_benchmark 1024000 5 0

DMLC_ENABLE_FABRIC=1 PS_VERBOSE=2 NUM_KEY_PER_SERVER=2 BYTEPS_USE_VEC=1 DMLC_ROLE=worker DMLC_EFA_DEBUG=1 ENABLE_RDMA_LOG=1 bash tests/local_multi_workers.sh 1 1 tests/test_benchmark 1024000 5 0

```
