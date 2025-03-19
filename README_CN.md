# PS-Lite 中文说明

这是PS-Lite参数服务器框架的修复版本，适配了现代Linux环境和编译器。

## 项目修复内容

1. **更新依赖库版本**
   - 更新Protobuf从3.5.1到3.19.4
   - 更新ZeroMQ从4.1.4到4.3.4
   - 修复下载链接

2. **修复编译器兼容性**
   - 添加对现代g++编译器(9.x及以上)的支持
   - 添加兼容性编译标志如-Wno-deprecated-copy和-Wno-class-memaccess
   - 修复类型转换和时间函数使用

3. **改进构建系统**
   - 更新CMake配置支持现代编译环境
   - 添加FindProtobuf模块以适应新版本API
   - 修复线程库链接问题
   - 添加测试框架

4. **测试脚本改进**
   - 整合所有测试脚本到统一的测试框架
   - 添加更详细的调试信息
   - 增加端口自动检测和分配功能
   - 改进测试结果报告

## 编译和安装

项目仅支持使用CMake构建：

```bash
# 创建并进入构建目录
mkdir -p build && cd build

# 配置项目
cmake ..

# 编译
make -j4
```

## 运行测试

项目提供了统一的测试脚本，位于`tests`目录：

```bash
# 进入测试目录
cd tests

# 运行所有测试
bash run_tests.sh

# 运行指定测试
bash run_tests.sh connection_test
bash run_tests.sh simple_app_test
bash run_tests.sh kv_app_test

# 运行多工作者测试
bash run_tests.sh multi_workers

# 仅设置测试环境
bash run_tests.sh setup

# 显示帮助信息
bash run_tests.sh help
```

测试脚本会自动设置测试环境、编译测试程序并运行测试。更多详情请参阅 [tests/README.md](tests/README.md)。

## 使用PS-Lite开发

要在您自己的项目中使用PS-Lite，请参考以下步骤：

1. 包含头文件
```cpp
#include "ps/ps.h"
```

2. 设置环境变量
```bash
export DMLC_NUM_WORKER=1  # 工作节点数量
export DMLC_NUM_SERVER=1  # 服务器节点数量
export DMLC_PS_ROOT_URI=127.0.0.1  # 调度器地址
export DMLC_PS_ROOT_PORT=8000  # 调度器端口
```

3. 启动PS-Lite系统
```cpp
#include "ps/ps.h"
using namespace ps;

int main(int argc, char *argv[]) {
  // 启动系统
  Start(0);
  
  // 根据角色执行不同逻辑
  if (IsScheduler()) {
    // 调度器逻辑
  } else if (IsWorker()) {
    // 工作节点逻辑
  } else if (IsServer()) {
    // 服务器节点逻辑
  }
  
  // 停止系统
  Finalize(0, true);
  return 0;
}
```

## 故障排除

如果遇到问题，请尝试以下步骤：

1. 确保所有节点都正确设置了环境变量
2. 检查防火墙设置，确保节点间可以通信
3. 运行带有详细日志的测试：`cd tests && bash run_tests.sh <测试名称>`
4. 查看 `./tmp/ps_lite_test/` 目录下的日志文件

### 网络和端口问题

如果遇到绑定端口失败的问题（如"bind failed"错误），可以尝试以下解决方案：

1. **端口已占用问题**
   - 测试脚本现在会自动选择可用端口
   - 使用 `netstat -tuln` 检查哪些端口可用
   - 手动设置 `DMLC_PS_ROOT_PORT` 环境变量为一个可用端口

2. **权限问题**
   - 确保您的用户有权限绑定到指定端口
   - 如果使用1024以下的端口，可能需要root权限
   - 考虑使用较高端口号（如10000以上）

3. **网络设置问题**
   - 检查防火墙规则，确保不阻止PS-Lite使用的端口
   - 在多机环境中，确保机器间网络互通

4. **测试个别组件**
   ```bash
   # 测试端口是否可用
   nc -zv 127.0.0.1 <端口号>
   
   # 检查IP配置
   ip addr show
   ```

## 项目结构

```
ps-lite/
├── build/         - 编译输出目录
├── deps/          - 依赖库目录
├── docs/          - 文档目录
├── include/       - 头文件目录
├── src/           - 源代码目录
├── tests/         - 测试文件目录
│   ├── README.md           - 测试说明文档
│   ├── run_tests.sh        - 统一测试脚本
│   ├── test_connection.cc  - 连接测试
│   ├── test_simple_app.cc  - 简单应用测试
│   ├── test_kv_app.cc      - KV应用测试
│   └── ...
├── tracker/       - 分布式跟踪器
├── CMakeLists.txt - CMake配置文件
├── LICENSE        - 许可证文件
└── README.md      - 英文说明文档
```

## 许可证

与原项目相同的许可证。 