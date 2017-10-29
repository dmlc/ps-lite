/**
 *  Copyright (c) 2015 by Contributors
 */
#include <unistd.h>
#include <thread>
#include <chrono>
#include "ps/internal/postoffice.h"
#include "ps/internal/message.h"
#include "ps/base.h"
#include "dmlc/DIME.h"
namespace ps {
	Postoffice::Postoffice() 
	{
		auto vanName = Environment::Get()->find("PSLITE_VAN_TYPE");
		if (vanName == NULL)
		{
			vanName = "zmq";
		}
		van_ = Van::Create(vanName);
		env_ref_ = Environment::_GetSharedRef();
		const char* val = NULL;
		val = CHECK_NOTNULL(Environment::Get()->find("DMLC_NUM_WORKER"));
		num_workers_ = atoi(val);
		val = CHECK_NOTNULL(Environment::Get()->find("DMLC_NUM_SERVER"));
		num_servers_ = atoi(val);
		val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
		std::string role(val);
		is_worker_ = role == "worker";
		is_server_ = role == "server";
		is_scheduler_ = role == "scheduler";
		verbose_ = GetEnv("PS_VERBOSE", 0);
	}
  bool Started = false;
	void Postoffice::Start(const char* argv0, const bool do_barrier) {
	  CHECK(Started == false);
	  Started = true;
	  const char* role_str = ps::Environment::Get()->find("DMLC_ROLE");
		auto IsServerNode = (role_str != nullptr) && (!strcmp(role_str, "server"));
		//if(IsServerNode)
		//  printf("[critical]server calling postoffice::start()\n");
		// init glog
		if (argv0) {
			dmlc::InitLogging(argv0);
		}
		else {
			dmlc::InitLogging("ps-lite\0");
		}

		// init node info.
		for (int i = 0; i < num_workers_; ++i) {
			int id = WorkerRankToID(i);
			for (int g : {id, kWorkerGroup, kWorkerGroup + kServerGroup,
				kWorkerGroup + kScheduler,
				kWorkerGroup + kServerGroup + kScheduler}) {
				node_ids_[g].push_back(id);
			}
		}

		for (int i = 0; i < num_servers_; ++i) {
			int id = ServerRankToID(i);
			for (int g : {id, kServerGroup, kWorkerGroup + kServerGroup,
				kServerGroup + kScheduler,
				kWorkerGroup + kServerGroup + kScheduler}) {
				node_ids_[g].push_back(id);
			}
		}

		for (int g : {kScheduler, kScheduler + kServerGroup + kWorkerGroup,
			kScheduler + kWorkerGroup, kScheduler + kServerGroup}) {
			node_ids_[g].push_back(kScheduler);
		}

		//if(IsServerNode)
		//printf("[unknown][unknown] setting up van\n");
		// start van
		van_->Start();

		// record start time
		start_time_ = time(NULL);

		// do a barrier here
		//raise(SIGTRAP);
		if (do_barrier) Barrier(kWorkerGroup + kServerGroup + kScheduler);
		//printf("[%d][%d] PostOffice is up and running.\n", van_->my_node().id, van_->my_node().role);
	}

  void Postoffice::Finalize(const bool do_barrier) {
	  // printf("[%d]Finalize1\n", van()->my_node().id);
	  //auto myid = van()->my_node().id;
	  if (do_barrier) Barrier(kWorkerGroup + kServerGroup + kScheduler,"finalizer");
	  //printf("[%d]Finalize2\n",myid);
	
		van_->Stop();
		//printf("[%d]Finalize3\n",myid);
	
		if (exit_callback_) exit_callback_();
		//printf("[%d]Finalize4\n",myid);
	
	}


	void Postoffice::AddCustomer(Customer* customer) {
		std::lock_guard<std::mutex> lk(mu_);
		int id = CHECK_NOTNULL(customer)->id();
		CHECK_EQ(customers_.count(id), (size_t)0) << "id " << id << " already exists";
		customers_[id] = customer;
	}


	void Postoffice::RemoveCustomer(Customer* customer) {
		std::lock_guard<std::mutex> lk(mu_);
		int id = CHECK_NOTNULL(customer)->id();
		customers_.erase(id);
	}


	Customer* Postoffice::GetCustomer(int id, int timeout) const {
		Customer* obj = nullptr;
		for (int i = 0; i < timeout * 1000 + 1; ++i) {
			{
				std::lock_guard<std::mutex> lk(mu_);
				const auto it = customers_.find(id);
				if (it != customers_.end()) {
					obj = it->second;
					break;
				}
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		return obj;
	}

  void Postoffice::Barrier(int node_group, const std::string id) {
    /* if(Postoffice::Get()->van()->my_node().role == 0)
      {
    printf("[%d][%d]requests barrier group=%d pl=%s\n",Postoffice::Get()->van()->my_node().id, Postoffice::Get()->van()->my_node().role,node_group,id.c_str());
    print_stacktrace();
    }*/
    
		if (GetNodeIDs(node_group).size() <= 1) return;
		auto role = van_->my_node().role;
		if (role == Node::SCHEDULER) {
			CHECK(node_group & kScheduler);
		}
		else if (role == Node::WORKER) {
			CHECK(node_group & kWorkerGroup);
		}
		else if (role == Node::SERVER) {
			CHECK(node_group & kServerGroup);
		}
		std::unique_lock<std::mutex> ulk(barrier_mu_);
		barrier_done_ = false;
		Message req;
		req.meta.recver = kScheduler;
		req.meta.request = true;
		req.meta.control.cmd = Control::BARRIER;
		req.meta.control.barrier_group = node_group;
		req.meta.timestamp = van_->GetTimestamp();
		req.meta.body = id;
		//if(Postoffice::Get()->van()->my_node().role == 2)
		//  printf("start of cnter\n");
		CHECK_GT(van_->Send(req), 0);
		//raise(SIGTRAP);
		//if(Postoffice::Get()->van()->my_node().role == 2)
		//  printf("end of cnter\n");
		barrier_cond_.wait(ulk, [this] {
			return barrier_done_;
		});
	}

	const std::vector<Range>& Postoffice::GetServerKeyRanges() {
		if (server_key_ranges_.empty()) {
			for (int i = 0; i < num_servers_; ++i) {
			    //non IB this way.
			    if(ps::Postoffice::Get()->van()->HasFeature(ps::Van::NativeInfiniband) == false)
			    {
				server_key_ranges_.push_back(Range(
					kMaxKey / num_servers_ * i,
					kMaxKey / num_servers_ * (i + 1)));
			    }
			    else
			    {
				server_key_ranges_.push_back(Range(0, kMaxKey));
			    }
			}
		}
		return server_key_ranges_;
	}

	void Postoffice::Manage(const Message& recv) {
		CHECK(!recv.meta.control.empty());
		const auto& ctrl = recv.meta.control;
		if (ctrl.cmd == Control::BARRIER && !recv.meta.request) {
			barrier_mu_.lock();
			barrier_done_ = true;
			barrier_mu_.unlock();
			barrier_cond_.notify_all();
		}
	}

	std::vector<int> Postoffice::GetDeadNodes(int t) {
		std::vector<int> dead_nodes;
		if (!van_->IsReady()) return dead_nodes;

		time_t curr_time = time(NULL);
		const auto& nodes = is_scheduler_
			? GetNodeIDs(kWorkerGroup + kServerGroup)
			: GetNodeIDs(kScheduler);
		{
			std::lock_guard<std::mutex> lk(heartbeat_mu_);
			for (int r : nodes) {
				auto it = heartbeats_.find(r);
				if ((it == heartbeats_.end() || it->second + t < curr_time)
					&& start_time_ + t < curr_time) {
					dead_nodes.push_back(r);
				}
			}
		}
		return dead_nodes;
	}
}  // namespace ps
