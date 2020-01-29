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
```

## Run Test
```
killall test_kv_app_benchmark; DMLC_EFA_DEBUG=1 PS_VERBOSE=1 ENABLE_RDMA_LOG=1 DMLC_ENABLE_FABRIC=1 bash tests/local_multi_workers.sh 1 1 tests/test_kv_app_benchmark 1024000 10 0
```
