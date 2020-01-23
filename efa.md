## Build with EFA
```
make clean; USE_FABRIC=1 make -j;
```

## Run Test
```
killall test_kv_app_benchmark; DMLC_EFA_DEBUG=1 PS_VERBOSE=1 ENABLE_RDMA_LOG=1 DMLC_ENABLE_FABRIC=1 bash tests/local_multi_workers.sh 1 1 tests/test_kv_app_benchmark 1024000 10 0
```
