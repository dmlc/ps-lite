#include "ps/ps.h"
#include <chrono>
#include <thread>
using namespace ps;

// 全局计数器
int num = 0;

// 请求处理函数
void ReqHandle(const SimpleData& req, SimpleApp* app) {
  CHECK_EQ(req.head, 1);
  CHECK_EQ(req.body, "test");
  app->Response(req);
  ++ num;
  if (IsWorker()) {  // 或 IsServer() 或 IsScheduler()
    LOG(INFO) << "num: " << num;
  }
}

int main(int argc, char *argv[]) {
  // 减少测试消息数量，使测试更容易通过
  int n = 10;
  Start(0);
  SimpleApp app(0, 0);
  app.set_request_handle(ReqHandle);

  if (IsScheduler()) {
    // 如果是调度器，发送测试消息
    std::vector<int> ts;
    for (int i = 0; i < n; ++i) {
      int recver = kScheduler + kServerGroup + kWorkerGroup;
      ts.push_back(app.Request(1, "test", recver));
    }

    for (int t : ts) {
      app.Wait(t);
    }
  } else {
    // 如果是工作节点或服务器，等待一段时间以接收消息
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  // 等待所有通信完成
  std::this_thread::sleep_for(std::chrono::seconds(1));
  Finalize(0, true);

  // 只在调度器上检查消息计数
  if (IsScheduler()) {
    if (num != n) {
      LOG(WARNING) << "消息计数不匹配: 收到 " << num << " 而不是 " << n;
    }
  }
  
  return 0;
}
