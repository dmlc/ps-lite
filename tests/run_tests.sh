#!/bin/bash
# PS-Lite 统一测试脚本
# 用法: 
#   ./run_tests.sh                  - 运行所有测试
#   ./run_tests.sh connection_test  - 运行指定测试
#   ./run_tests.sh multi_workers    - 运行多工作者测试
#   ./run_tests.sh setup            - 仅设置测试环境

# 设置颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 确保在项目根目录
cd $(dirname $0)/..
PROJECT_ROOT=$(pwd)

# 设置环境变量
setup_env() {
  echo -e "${BLUE}===== 设置测试环境 =====${NC}"
  
  # 1. 创建环境变量文件
  echo "创建环境变量设置..."
  cat > env.sh << EOF
#!/bin/bash
# PS-Lite环境变量设置
export LD_LIBRARY_PATH=\$(pwd)/deps/lib:\$LD_LIBRARY_PATH
export PATH=\$(pwd)/deps/bin:\$PATH
EOF
  chmod +x env.sh
  
  # 2. 加载环境变量
  source ./env.sh
  
  # 3. 清理并重建编译目录
  echo "清理并重建编译目录..."
  rm -rf build
  mkdir -p build
  cd build
  
  # 4. 使用cmake配置项目
  echo "配置CMake..."
  cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
    -DProtobuf_ROOT=$(pwd)/../deps \
    -DZeroMQ_ROOT=$(pwd)/../deps 2>/dev/null
  
  # 5. 编译项目
  echo "编译项目..."
  make -j4 2>/dev/null
  
  cd ..
  echo -e "${GREEN}===== 环境设置完成 =====${NC}"
}

# 查找可用端口
find_free_port() {
  local port=0
  while [ $port -lt 1024 ]; do
    port=$(shuf -i 10000-65535 -n 1 2>/dev/null || echo $((10000 + RANDOM % 55535)))
    
    # 检查端口是否可用
    if command -v netstat >/dev/null 2>&1; then
      netstat -tuln | grep -q ":$port " || break
    elif command -v ss >/dev/null 2>&1; then
      ss -tuln | grep -q ":$port " || break
    else
      # 如果netstat和ss都不可用
      break
    fi
  done
  echo $port
}

# 运行单个测试
run_single_test() {
  local test_name=$1
  
  # 创建临时目录用于日志
  LOG_DIR=./tmp/ps_lite_test
  mkdir -p $LOG_DIR
  rm -f $LOG_DIR/*
  
  # 确保编译
  echo "编译测试: $test_name"
  cd build && make -j4 $test_name >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo -e "${RED}编译失败!${NC}"
    cd ..
    return 1
  fi
  cd ..
  
  # 设置基本环境变量
  export DMLC_NUM_SERVER=1
  export DMLC_PS_ROOT_URI=127.0.0.1
  export DMLC_PS_ROOT_PORT=$(find_free_port)
  export PS_VERBOSE=1
  export DMLC_LOG_LEVEL=0
  
  # 如果是多工作者测试，设置DMLC_NUM_WORKER=2
  if [[ "$test_name" == *"multi_workers"* ]]; then
    export DMLC_NUM_WORKER=2
    echo "检测到多工作者测试，设置DMLC_NUM_WORKER=2"
  else
    export DMLC_NUM_WORKER=1
  fi
  
  echo "使用端口: $DMLC_PS_ROOT_PORT"
  
  # 为多工作者测试准备参数
  if [[ "$test_name" == *"multi_workers"* ]]; then
    SCHEDULER_ARG="scheduler"
    SERVER_ARG="server"
    WORKER_ARG="worker"
  else
    SCHEDULER_ARG=""
    SERVER_ARG=""
    WORKER_ARG=""
  fi
  
  echo "1. 启动scheduler..."
  export DMLC_ROLE=scheduler
  build/tests/$test_name $SCHEDULER_ARG > $LOG_DIR/scheduler.log 2>&1 &
  SCHEDULER_PID=$!
  
  # 等待scheduler启动
  sleep 2
  if ! ps -p $SCHEDULER_PID > /dev/null; then
    echo -e "${RED}Scheduler启动失败，查看日志:${NC}"
    cat $LOG_DIR/scheduler.log
    return 1
  fi
  echo "Scheduler已启动 (PID: $SCHEDULER_PID)"
  
  # 启动多个servers
  for i in $(seq 0 $((DMLC_NUM_SERVER-1))); do
    echo "2. 启动server $i..."
    export DMLC_ROLE=server
    export DMLC_SERVER_ID=$i
    build/tests/$test_name $SERVER_ARG > $LOG_DIR/server_$i.log 2>&1 &
    SERVER_PID[$i]=$!
    echo "Server $i 已启动 (PID: ${SERVER_PID[$i]})"
    sleep 1
  done
  
  # 启动多个workers
  for i in $(seq 0 $((DMLC_NUM_WORKER-1))); do
    echo "3. 启动worker $i..."
    export DMLC_ROLE=worker
    export DMLC_WORKER_ID=$i
    build/tests/$test_name $WORKER_ARG > $LOG_DIR/worker_$i.log 2>&1 &
    WORKER_PID[$i]=$!
    echo "Worker $i 已启动 (PID: ${WORKER_PID[$i]})"
    sleep 1
  done
  
  # 等待所有进程结束
  echo "等待所有进程完成..."
  wait_with_timeout() {
    local pid=$1
    local timeout=${2:-60}  # 默认60秒超时
    
    local counter=0
    while ps -p $pid > /dev/null && [ $counter -lt $timeout ]; do
      sleep 1
      counter=$((counter+1))
    done
    
    if ps -p $pid > /dev/null; then
      echo "进程 $pid 超时，将被强制终止"
      return 1
    fi
    return 0
  }
  
  # 等待所有worker完成
  for i in $(seq 0 $((DMLC_NUM_WORKER-1))); do
    if ! wait_with_timeout ${WORKER_PID[$i]} 120; then
      kill ${WORKER_PID[$i]} || true
    fi
  done
  
  # 清理
  echo "测试完成，清理进程..."
  kill $SCHEDULER_PID 2>/dev/null || true
  for i in $(seq 0 $((DMLC_NUM_SERVER-1))); do
    kill ${SERVER_PID[$i]} 2>/dev/null || true
  done
  
  sleep 1
  
  # 检查日志是否有错误
  local error_found=0
  
  if grep -i "error\|fail\|exception\|segmentation\|terminate" $LOG_DIR/scheduler.log; then
    echo -e "${RED}Scheduler日志中发现错误${NC}"
    error_found=1
  fi
  
  for i in $(seq 0 $((DMLC_NUM_SERVER-1))); do
    if grep -i "error\|fail\|exception\|segmentation\|terminate" $LOG_DIR/server_$i.log; then
      echo -e "${RED}Server $i 日志中发现错误${NC}"
      error_found=1
    fi
  done
  
  for i in $(seq 0 $((DMLC_NUM_WORKER-1))); do
    if grep -i "error\|fail\|exception\|segmentation\|terminate" $LOG_DIR/worker_$i.log; then
      echo -e "${RED}Worker $i 日志中发现错误${NC}"
      error_found=1
    fi
  done
  
  if [ $error_found -eq 0 ]; then
    echo -e "${GREEN}测试 $test_name 成功完成!${NC}"
    return 0
  else
    echo -e "${RED}测试 $test_name 失败，检查日志目录: $LOG_DIR${NC}"
    return 1
  fi
}

# 运行多工作者测试
run_multi_workers_test() {
  # 创建临时目录用于日志
  mkdir -p ./tmp/logs
  rm -f ./tmp/logs/*
  
  # 确保测试已编译
  echo "编译测试..."
  cd build && make -j4 kv_app_multi_workers_test >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo -e "${RED}编译失败!${NC}"
    cd ..
    return 1
  fi
  cd ..
  
  # 设置环境变量
  export DMLC_NUM_SERVER=1
  export DMLC_NUM_WORKER=2
  export DMLC_PS_ROOT_URI='127.0.0.1'
  export DMLC_PS_ROOT_PORT=$(find_free_port)
  export PS_VERBOSE=1
  
  echo "使用端口: $DMLC_PS_ROOT_PORT"
  
  # 启动scheduler
  echo "1. 启动scheduler..."
  export DMLC_ROLE='scheduler'
  build/tests/kv_app_multi_workers_test scheduler > ./tmp/logs/scheduler.log 2>&1 &
  SCHEDULER_PID=$!
  
  # 等待scheduler启动
  sleep 2
  if ! ps -p $SCHEDULER_PID > /dev/null; then
    echo -e "${RED}Scheduler启动失败，查看日志:${NC}"
    cat ./tmp/logs/scheduler.log
    return 1
  fi
  echo "Scheduler已启动 (PID: $SCHEDULER_PID)"
  
  # 启动server
  echo "2. 启动server..."
  export DMLC_ROLE='server'
  build/tests/kv_app_multi_workers_test server > ./tmp/logs/server.log 2>&1 &
  SERVER_PID=$!
  echo "Server已启动 (PID: $SERVER_PID)"
  sleep 1
  
  # 启动worker
  echo "3. 启动worker..."
  export DMLC_ROLE='worker'
  build/tests/kv_app_multi_workers_test worker > ./tmp/logs/worker.log 2>&1 
  WORKER_RESULT=$?
  
  # 清理进程
  echo "测试完成，清理进程..."
  kill $SCHEDULER_PID 2>/dev/null || true
  kill $SERVER_PID 2>/dev/null || true
  
  # 检查结果
  if [ $WORKER_RESULT -eq 0 ]; then
    echo -e "${GREEN}多工作者测试成功完成!${NC}"
    return 0
  else
    echo -e "${RED}多工作者测试失败! 查看日志:${NC}"
    cat ./tmp/logs/worker.log
    return 1
  fi
}

# 运行所有测试
run_all_tests() {
  echo -e "${GREEN}===== 开始PS-Lite测试 =====${NC}"
  
  # 跟踪测试结果
  local total_tests=5
  local passed_tests=0
  
  run_test() {
    local test_name=$1
    local test_cmd=$2
    local test_num=$3
    
    echo -e "\n${YELLOW}[$test_num/$total_tests] 运行 $test_name...${NC}"
    eval $test_cmd
    
    if [ $? -eq 0 ]; then
      echo -e "${GREEN}✓ $test_name 测试通过!${NC}"
      passed_tests=$((passed_tests+1))
      return 0
    else
      echo -e "${RED}✗ $test_name 测试失败!${NC}"
      return 1
    fi
  }
  
  # 1. 运行连接测试
  run_test "连接测试" "run_single_test connection_test" 1
  connection_result=$?
  
  # 2. 运行简单应用测试
  run_test "简单应用测试" "run_single_test simple_app_test" 2
  simple_app_result=$?
  
  # 3. 运行KV应用测试
  run_test "KV应用测试" "run_single_test kv_app_test" 3
  kv_app_result=$?
  
  # 4. 运行KV应用基准测试
  run_test "KV应用基准测试" "run_single_test kv_app_benchmark_test" 4
  kv_benchmark_result=$?
  
  # 5. 运行多工作者测试
  run_test "多工作者测试" "run_multi_workers_test" 5
  multi_workers_result=$?
  
  # 总结测试结果
  echo -e "\n${GREEN}===== 测试结果摘要 =====${NC}"
  echo -e "总测试数: $total_tests"
  echo -e "通过测试: $passed_tests"
  echo -e "失败测试: $((total_tests-passed_tests))"
  
  if [ $passed_tests -eq $total_tests ]; then
    echo -e "\n${GREEN}===== 所有测试通过! =====${NC}"
    return 0
  else
    echo -e "\n${RED}===== 部分测试失败 =====${NC}"
    # 列出失败的测试
    [ $connection_result -ne 0 ] && echo -e "${RED}✗ 连接测试${NC}"
    [ $simple_app_result -ne 0 ] && echo -e "${RED}✗ 简单应用测试${NC}"
    [ $kv_app_result -ne 0 ] && echo -e "${RED}✗ KV应用测试${NC}"
    [ $kv_benchmark_result -ne 0 ] && echo -e "${RED}✗ KV应用基准测试${NC}"
    [ $multi_workers_result -ne 0 ] && echo -e "${RED}✗ 多工作者测试${NC}"
    return 1
  fi
}

# 显示帮助信息
show_help() {
  echo -e "${BLUE}PS-Lite 测试脚本${NC}"
  echo -e "用法:"
  echo -e "  ${YELLOW}./run_tests.sh${NC}                  - 运行所有测试"
  echo -e "  ${YELLOW}./run_tests.sh connection_test${NC}  - 运行指定测试"
  echo -e "  ${YELLOW}./run_tests.sh multi_workers${NC}    - 运行多工作者测试"
  echo -e "  ${YELLOW}./run_tests.sh setup${NC}            - 仅设置测试环境"
  echo -e "  ${YELLOW}./run_tests.sh help${NC}             - 显示此帮助信息"
  echo
  echo -e "可用的测试:"
  echo -e "  ${GREEN}connection_test${NC}        - 基本连接测试"
  echo -e "  ${GREEN}simple_app_test${NC}        - 简单应用测试"
  echo -e "  ${GREEN}kv_app_test${NC}            - KV应用测试"
  echo -e "  ${GREEN}kv_app_benchmark_test${NC}  - KV应用基准测试"
  echo -e "  ${GREEN}multi_workers${NC}          - 多工作者KV应用测试"
}

# 主函数
main() {
  # 如果没有参数，运行所有测试
  if [ $# -eq 0 ]; then
    setup_env
    run_all_tests
    exit $?
  fi
  
  # 处理参数
  case "$1" in
    "help"|"-h"|"--help")
      show_help
      ;;
    "setup")
      setup_env
      ;;
    "multi_workers")
      setup_env
      run_multi_workers_test
      exit $?
      ;;
    "all")
      setup_env
      run_all_tests
      exit $?
      ;;
    *)
      # 检查是否为有效测试名称
      if [ -f "build/tests/$1" ] || [ -f "tests/test_$1.cc" ]; then
        setup_env
        run_single_test $1
        exit $?
      else
        echo -e "${RED}错误: 未知的测试 '$1'${NC}"
        show_help
        exit 1
      fi
      ;;
  esac
}

# 执行主函数
main "$@" 