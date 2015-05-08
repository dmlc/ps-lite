#include "ps.h"
typedef float Val;
namespace ps {

DECLARE_int32(num_workers);
class MyHandle {
 public:
  void SetCaller(void *obj) { obj_ = (Customer*)obj; }

  inline void Start(bool push, int timestamp, const std::string& worker) {
    std::cout << "accept " << (push ? "push" : "pull") << " from " << worker
              << " with timestamp " << timestamp << std::endl;
    ts_ = timestamp;
  }

  inline void Finish() {
    std::cout << "finished " << obj_->NumDoneReceivedRequest(ts_, kWorkerGroup)
              << " / " << FLAGS_num_workers << " on timestamp " << ts_ << std::endl;
  }

  inline void Init(Blob<const Key> keys,
                   Blob<Val> vals) {
    memset(vals.data, 0, vals.size*sizeof(Val));
    std::cout << "init key " << keys << " val " << vals << std::endl;
  }

  inline void Push(Blob<const Key> recv_keys,
                   Blob<const Val> recv_vals,
                   Blob<Val> my_vals) {
    for (size_t i = 0; i < recv_vals.size; ++i)
      my_vals[i] += recv_vals[i];
    std::cout << "handle push: key " << recv_keys << " val " << recv_vals << std::endl;
  }

  inline void Pull(Blob<const Key> recv_keys,
                   Blob<const Val> my_vals,
                   Blob<Val> send_vals) {
    for (size_t i = 0; i < my_vals.size; ++i)
      send_vals[i] = my_vals[i];
    std::cout << "handle pull: key " << recv_keys << std::endl;
  }
 private:
  Customer* obj_ = nullptr;
  int ts_;
};

}  // namespace ps
