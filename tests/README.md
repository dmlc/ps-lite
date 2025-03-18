# PS-Lite 测试指南

## 运行测试

所有测试功能已整合到一个脚本中，无需额外设置环境：

```bash
# 在测试目录运行
cd tests
bash run_tests.sh
```

## 常用命令

```bash
# 运行所有测试
bash run_tests.sh

# 运行指定测试
bash run_tests.sh connection_test
bash run_tests.sh simple_app_test
bash run_tests.sh kv_app_test
bash run_tests.sh kv_app_benchmark_test
bash run_tests.sh multi_workers

# 仅设置测试环境
bash run_tests.sh setup

# 显示帮助信息
bash run_tests.sh help
```

## 可用测试

- `connection_test` - 基本连接测试
- `simple_app_test` - 简单应用测试
- `kv_app_test` - KV应用测试
- `kv_app_benchmark_test` - KV应用基准测试
- `multi_workers` - 多工作者KV应用测试
