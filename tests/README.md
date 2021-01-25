How to use

build by `make test` in the root directory, then run

```bash
# GPU <-> CPU push_pull
NODE_ONE_IP=xxx NODE_TWO_IP=yyy BENCHMARK_NTHREAD=1 UCX_RNDV_SCHEME=put_zcopy SKIP_DEV_ID_CHECK=1 TEST_ENABLE_CPU=0 BINARY="./test_benchmark" ARGS="4096000 999999999 1" LOCAL_SIZE=2 bash ./ucx_multi_node.sh
# CPU <-> CPU push_pull
NODE_ONE_IP=xxx NODE_TWO_IP=yyy BENCHMARK_NTHREAD=1 UCX_RNDV_SCHEME=put_zcopy SKIP_DEV_ID_CHECK=1 TEST_ENABLE_CPU=1 BINARY="./test_benchmark" ARGS="4096000 999999999 1" LOCAL_SIZE=0 bash ./ucx_multi_node.sh
# CPU <-> CPU gather scatter
NODE_ONE_IP=xxx NODE_TWO_IP=yyy DMLC_NUM_WORKER=2 UCX_RNDV_SCHEME=put_zcopy bash ./ucx_multi_node.sh
# local CPU multi-port
DMLC_NODE_HOST=xxx bash run_benchmark.sh `ROLE`
```
