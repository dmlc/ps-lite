## Build with libfabric for elastic fabric accelerator

AMI: Base Deep Learning AMI (ubuntu/AML)

1. install gcc-4.9 first

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

2. build ps-lite
```
make clean; USE_FABRIC=1 make -j;
```

3. run tests
```
DMLC_INTERFACE=eth0 DMLC_PS_ROOT_URI=ROOT_IP DMLC_ENABLE_RDMA=fabric bash tests/local_multi_workers.sh 1 1 tests/test_benchmark 4096000 100 1
```
