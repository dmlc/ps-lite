#pragma once
#include "infiniband_van.h"
#include "dmlc/logging.h"
#include <thread>
#include <vector>
#include "Verbs.hpp"
#include <atomic>
#include <sstream> 
#include <math.h>
#include "lfq.h"
#include "dmlc/DIME.h"
#include "PHUB.h"
#include "Optimizers.h"
#include "Helpers.h"
using namespace std;
namespace ps
{
    //struct atomicwrapper
    //{
    //    std::atomic_int core{ 0 };
    //    atomicwrapper &operator=(const atomicwrapper &other)
    //    {
    //        core.store(other.core.load());
    //    }
    //    atomicwrapper() :core() {}
    //    atomicwrapper(const std::atomic_int &a) :core(a.load()) {}
    //    atomicwrapper(const atomicwrapper &other) :core(other.core.load()) {}
    //};
    class PSHUBVan :public InfiniBandVan
    {
    public:
        int MaximumAllowedThreads;
        //one for each key.
        vector<vector<PHubMergeBuffer>> PerSocketMergeBuffers;
        bool SuppressAggregator = false;
        //vector<bool> isKeyInit;
        //looks like timestamp is th eonly thing that changes.
        //sender to timestamp mapping.
        vector<atomic_int*> UpdatesReceivedBuffer;
        //how many copies of data is merged for the same key?
        std::atomic<int> ai{ 0 };
        bool IsAsync = false;
        bool SuppressOptimizer = false;
        Optimizer* OPT = NULL;
        Aggregator*     ADD = NULL;
        //let infiniband van deal with all those start up thingy.
        //leave send as it is.
        //intercept push calls 
        PSHUBVan(int maxDataServingThreads)
        {
            auto async = Environment::Get()->find("PHUB_ASYNC_MODE");
            if (async != NULL)
            {
                IsAsync = true;
            }
            auto suppressOpt = Environment::Get()->find("PHUB_SUPPRESS_OPTIMIZER");
            if (suppressOpt != NULL)
            {
                SuppressOptimizer = true;
            }
            auto suppressAgg = Environment::Get()->find("PHUB_SUPPRESS_AGGREGATOR");
            if (suppressAgg != NULL)
            {
                SuppressAggregator = true;
            }

            //our job is to distribute these threads to different jobs.
            this->MaximumAllowedThreads = maxDataServingThreads;
            FeatureSet = Van::WorkerSidePushPullZeroCopy |
                Van::OnlyDeliversMergedBuffer |
                Van::SupportsKeyChunking |
                Van::NativeInfiniband |
                Van::VerbsPostSendNoLocking |
                Van::SupportsBitwidthSIM |
                Van::PullRequestElision |
                Van::MetadataElision;
        }
        ~PSHUBVan()
        {
            //what is this??
            int  tst_val = 0;
            int  new_val = 1;
            auto exchanged = ai.compare_exchange_strong(tst_val, new_val);
            if (exchanged == false) return;
        }
        std::vector<std::thread> threads;
        //things should start after OnKeyPopulated.
        //For SendMsg/RecvMsg we dont care. 
        //Override only RecvMsg.
        std::string ShowImbalance()
        {
            std::stringstream ss;
            ss << "Proc Workloads:";
            std::vector<size_t> loads;
            loads.resize(GetUnderlyingWorkerThreadCount());
	    
	    std::vector<size_t> IFLoads;
	    IFLoads.resize(verbs->virtual_devices.size());
	    std::vector<size_t> CQKeys;
	    CQKeys.resize(verbs->sendCompletionQueues.size());
            for (size_t i = 0; i < machineCount; i++)
            {
                for (size_t j = 0; j < keySize.size(); j++)
                {
                    auto qp = verbs->workerKey2QPIdx.at(j);
                    if (verbs->workerQP2SrvId.at(qp) == ps::Postoffice::Get()->my_rank())
                    {
                        auto& ep = verbs->Helper_Server_GetEndpointFromKey(j, i);
                        auto coreIdx = ep.CoreIdx;
                        loads[coreIdx] += keySize[j];
			auto ifIdx = ep.DeviceIdx;
			IFLoads.at(ifIdx) += keySize[j];
			auto idx = ep.CQIdx;
			CQKeys.at(idx)++;
                    }
                }
            }
            for (int i = 0; i < GetUnderlyingWorkerThreadCount(); i++)
            {
                ss << " [Proc " << i << "]:" << loads[i] / 1024.0 / 1024.0 << " MB";
                //which key is mapped to a CQ?
            }

	    ss << " Interface Workloads:";
	    for (size_t i = 0; i < IFLoads.size(); i++)
	    {
		ss << " ["<< verbs->device_names.at(i) << ":" << verbs->ports.at(i) << "]" << IFLoads[i] / 1024.0 / 1024.0 << " MB";
	    }
	    ss << " Max CQ Load = " << *std::max_element(CQKeys.begin(), CQKeys.end());
            return ss.str();
        }

        void virtual InitializeVanOptimizerValues(uint num, float* values) override
        {
            if (my_node().role == Node::SERVER)
            {
                CHECK(OPT != NULL || SuppressOptimizer);
                if (OPT != NULL)
                {
                    OPT->PopulateTrainingParams(num, values);
                }
            }
        }

        void OnKeyPopulated() override
        {
            InfiniBandVan::OnKeyPopulated();
            if (my_node().role != Node::SERVER) return;
            //CHECK(verbs->CoreCount > 0);

            auto prefetch_dist_str = Environment::Get()->find("PHUB_PREFETCH_DIST");
            size_t prefetch_distance = 0x240;
            if (prefetch_dist_str != NULL)
            {
                prefetch_distance = atoi(prefetch_dist_str);
            }

            auto optimizer = Environment::Get()->find("PHUB_OPTIMIZER");
            if (optimizer == NULL)
            {
                optimizer = "nagntnt";
            }
            OPT = Optimizer::Create(optimizer, machineCount, keySize, prefetch_distance, verbs);
            CHECK_NOTNULL(OPT);

            auto aggregator = Environment::Get()->find("PHUB_AGGREGATOR");
            if (aggregator == NULL)
            {
                aggregator = "ntnt";
            }
            ADD = Aggregator::Create(aggregator, prefetch_distance);
            CHECK_NOTNULL(ADD);

            /*if (MaximumAllowedThreads > keySize.size())
            {
                MaximumAllowedThreads = keySize.size();
        }*/
        //UpdatesReceivedBuffer.resize(keySize.size());
        //per socket aggregation.
	    //use migration to perform resize.
	    for(size_t sockId = 0; sockId < verbs->SocketCount; sockId++)
	    {
		MigrateToNumaNode(sockId);
		PerSocketMergeBuffers.push_back(vector<PHubMergeBuffer>(keySize.size()));
		//make sure the last one's address is on the right numa node.
		if(PerSocketMergeBuffers.size() > 0 && GetAddressNumaNode(&PerSocketMergeBuffers.back()) != sockId)
		{
		    LOG(INFO) << " Per socket merge buffer allocation per sock = " << sockId << " failed to allocate on desired socket.";
		}
	    }
            //PerSocketMergeBuffers.resize(verbs->SocketCount, vector<PHubMergeBuffer>(keySize.size()));

            //isKeyInit.resize(keySize.size(),true);
            //resize each vector in merge buffer.
            int maxKeySizeinB = 0;
            Key kid = -1;
            size_t totalSizeInMB = 0;
            auto allocator = PHubAllocator::Get();
            CHECK(allocator->IsInitialized());
            size_t len = 0;
            UpdatesReceivedBuffer.resize(keySize.size(), NULL);
            for (size_t i = 0; i < keySize.size(); i++)
            {
                //if its DirectConnect, UpdatesReceivedBuffer is allocated in one of the NUMA node.
                if (verbs->DirectConnect)
                {
                    UpdatesReceivedBuffer.at(i) = new atomic_int(0);
                    CHECK(*(UpdatesReceivedBuffer.back()) == 0);
                }
                //is this key for me?
                auto wQP = verbs->workerKey2QPIdx[i];
                auto target = verbs->workerQP2SrvId[wQP];
                if (target != ps::Postoffice::Get()->IDtoRank(my_node_.id))
                {
                    //printf("[%d] key %d is assigned to %d, not me\n", ps::Postoffice::Get()->my_rank(), i, target);
                    continue;
                    //my idx is calculated this way.
                }
                CHECK(keySize.find(i) != keySize.end());
                //for RDMA only:
                //send in recvBuffer's meta address.
                //send in remote's kv buffer.
                //send in remote's meta buffer.
                std::vector<uint64_t> rdmaRecvMetaBuffersPerWorker(machineCount, 0);
                std::vector<uint64_t> rdmaRemoteWorkerKVBufferAddr(machineCount, 0);
                std::vector<uint64_t> rdmaRemoteWorkerMetaBufferAddr(machineCount, 0);
                //populate those 3 vectors.
                CHECK(machineCount == ps::Postoffice::Get()->num_workers());

                //when direct connect is disabled, the per socket merge buffer collapse into one single buffer.
                for (int sid = 0; sid < PerSocketMergeBuffers.size(); sid++)
                {
                    for (size_t mid = 0; mid < machineCount; mid++)
                    {
                        size_t len;
                        rdmaRecvMetaBuffersPerWorker[mid] = (uint64_t)allocator->PHUBReceiveMetaBuffer(i, mid, sid, len);
                        rdmaRemoteWorkerKVBufferAddr[mid] = (uint64_t)remoteKVAddrs.at(mid)[i];
                        CHECK(rdmaRemoteWorkerKVBufferAddr[mid] != 0);
                        rdmaRemoteWorkerMetaBufferAddr[mid] = (uint64_t)remoteMetaAddrs.at(mid)[i];
                        CHECK(rdmaRemoteWorkerMetaBufferAddr[mid] != 0);
                    }
                    //auto& ep = verbs->Helper_Server_GetEndpointFromKey(i, 0);
                    //CHECK(sid == ep.SocketIdx);
                    PerSocketMergeBuffers.at(sid).at(i).Init(verbs,
                        keySize[i] + sizeof(MetaSlim),
                        i,
                        //note that this assignment is only valid for switch-enabled.
                        //direct connect requires overwriting LKey when pushing out because it's different interface.
                        //there is only 1 copy
                        (char*)allocator->PHUBMergeMetaBuffer(i, 0, sid, len),
                        //not used
                        (char*)allocator->PHUBMergeMetaBuffer1(i, 0, sid, len),
                        rdmaRecvMetaBuffersPerWorker,
                        rdmaRemoteWorkerKVBufferAddr,
                        rdmaRemoteWorkerMetaBufferAddr);
                    PerSocketMergeBuffers.at(sid).at(i).PSInitialized = true;
                    allocator->VerifyPHUBMergeKV1(sid, i, PerSocketMergeBuffers[sid][i].GetCurrentReadBuffer(), PerSocketMergeBuffers[sid][i].GetCurrentReadBuffer() + PerSocketMergeBuffers[sid][i].GetKVBufferLenPaddedForSSE());
                    allocator->VerifyPHUBMergeKV(sid, i, PerSocketMergeBuffers[sid][i].GetCurrentWriteBuffer(), PerSocketMergeBuffers[sid][i].GetCurrentWriteBuffer() + PerSocketMergeBuffers[sid][i].GetKVBufferLenPaddedForSSE());
                    allocator->VerifyPHUBMergeMeta(sid, i, PerSocketMergeBuffers[sid][i].Buffer1, PerSocketMergeBuffers[sid][i].Buffer1 + sizeof(MetaSlim));
                    allocator->VerifyPHUBMergeMeta1(sid, i, PerSocketMergeBuffers[sid][i].Buffer2, PerSocketMergeBuffers[sid][i].Buffer2 + sizeof(MetaSlim));
                    CHECK(PerSocketMergeBuffers[sid][i].GetCurrentReadBuffer() == PerSocketMergeBuffers[sid][i].Buffer2 + sizeof(MetaSlim));
                    CHECK(PerSocketMergeBuffers[sid][i].GetCurrentWriteBuffer() == PerSocketMergeBuffers[sid][i].Buffer1 + sizeof(MetaSlim));
                    if (initializer.find(i) != initializer.end())
                    {
                        //some tests may not have this.
                        CHECK(initializer[i].size() * sizeof(float) == keySize[i]);
                        memcpy(PerSocketMergeBuffers.at(sid).at(i).GetCurrentReadBuffer(), initializer[i].data(), keySize[i]);
                    }
                    else
                    {
                        //CHECK(false);
                        memset(PerSocketMergeBuffers.at(sid).at(i).GetCurrentReadBuffer(), 0, keySize[i]);
                    }
                }

                if (keySize[i] > maxKeySizeinB)
                {
                    maxKeySizeinB = keySize[i];
                    kid = i;
                }
                /*auto j = i;
                if(j == 0 && keySize[j] / sizeof(float) > 60000)
                {
                    printf("[INIT0] %s\n", SummarizeContinousBuffer((float*)MergeBuffers[0].GetCurrentReadBuffer(), 60000).c_str());
                    printf("[INIT1] %s\n", SummarizeContinousBuffer((float*)MergeBuffers[0].GetCurrentReadBuffer() + 60000, keySize[0] / sizeof(float)-60000).c_str());
                }
                else if((j == 0 || j == 1) && keySize[0] / sizeof(float) == 60000)
                {
                    printf("[INIT%d] %s\n",j, SummarizeContinousBuffer((float*)MergeBuffers[j].GetCurrentReadBuffer(), keySize[j] / sizeof(float)).c_str());
                    }*/

                totalSizeInMB += keySize[i];
            }

            auto hc = std::thread::hardware_concurrency();
            //how many cores in real sockets.
            auto perSocket = hc / verbs->SocketCount;
            //how many in virtual sockets
            auto perSocketAdjusted = MaximumAllowedThreads / verbs->SocketCount;
            CHECK(perSocket * verbs->SocketCount == hc) << "Do you have an unbalanced socket? ";
            CHECK(perSocketAdjusted * verbs->SocketCount == MaximumAllowedThreads) << "Cannot evenly divide requested number of processors to each sockets";
            CHECK(MaximumAllowedThreads <= hc) << "You are requesting more threads than there are cores.";
            for (size_t i = 0; i < MaximumAllowedThreads; i++)
            {
                int socketId = i / perSocketAdjusted;
                int socketOffset = i % perSocketAdjusted;
                int cpuid = socketId * perSocket + socketOffset;
                threads.push_back(thread(&PSHUBVan::infiniBandCollect, this, i));
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(cpuid, &cpuset);
                int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
                if (rc != 0)
                {
                    printf("[PSHUB][%d] Thread affinity for %d has failed\n", my_node_.id, i);
		    CHECK(false);
                }
                else
                {
                    //printf("[PSHUB] mapping thread %d to cpuid %d, in socket = %d\n", i, cpuid, socketId);
                }
            }
	    GTG = true;
            PS_VLOG(0) << "[PSHUB" << my_node_.id << "] Maximum Key=" << kid << " Size=" << maxKeySizeinB / 1024.0 << " kB. There are " << keySize.size() << " keys. Total: " << totalSizeInMB / 1024.0 / 1024.0 << " MB. Running Mode: Async = " << IsAsync << " Suppress Optimizer = " << SuppressOptimizer << " Optimizer = " << optimizer << " Aggregator = " << aggregator << " Suppress Aggregator =" << SuppressAggregator << " " << ShowImbalance();

        }
        volatile bool stopRequested = false;
        std::string DebugString(std::vector<pair<int, int>>& data)
        {
            std::stringstream ss;
            ss << "disect:\n";
            for (auto item : data)
            {
                ss << "(" << item.first << "," << item.second << ")";
            }
            ss << "\n";
            return ss.str();
        }
        std::unordered_map<Key, std::vector<float>> initializer;

        virtual int RecvMsg(Message* msg) override
        {
            if (my_node_.role == Node::WORKER)
            {
                return InfiniBandVan::RecvMsg(msg);
            }
            else if (my_node_.role == Node::SCHEDULER)
            {
                return ZMQVan::RecvMsg(msg);
            }
            else
            {
                //intercept init data
                //completely detach PHUB with PS-LITE pipeline
                //we need to intercept initialization data packages.
                while (true)
                {
                    auto ret = ZMQVan::RecvMsg(msg);
                    if (msg->meta.control.empty() && msg->data.size() == 3)
                    {
                        //this is initialization
                        auto key = SArray<Key>(msg->data[0])[0];
                        if (key >= 0)
                        {
                            auto val = SArray<float>(msg->data[1]);
                            auto len = SArray<int>(msg->data[2])[0];
                            CHECK(msg->data.size() == 3);
                            //CHECK(msg->data[2].size() == 1);
                            CHECK(FullyInitialized() == false) << " Infiniband must not be fully initialized.";
                            CHECK(keySize[key] == len * sizeof(float));
                            auto& vec = initializer[key];
                            vec.resize(len);
                            memcpy(vec.data(), val.data(), len * sizeof(float));
                            //need to quickly acknowledge the sender
                            /*Message ack;
                            ack.data.clear();
                            ack.meta.sender = my_node().id;
                            ack.meta.recver = msg->meta.sender;
                            ack.meta.request = 0;
                            ack.meta.simple_app = 0;
                            ack.meta.customer_id = 0;
                            ack.meta.timestamp = msg->meta.timestamp;
                            ack.meta.head = 0;*/
                            //done.
                            return ret;
                        }
                        else
                        {
                            //an init.
                            //I could have intercepted it here. but...
                            CHECK(msg->meta.control.cmd == Control::INFINIBANDKEYSIZEEXCHG);
                        }
                    }
                    /* else if (msg->meta.simple_app == 1 && msg->meta.control.empty() && msg->meta.body.size() > 0) */
                    /* { */
                    /*   //void Van::UnpackMeta(const char* meta_buf, int buf_size, Meta* meta) { */

                    /* } */
                    else
                    {
                        return ret;
                    }
                }
            }
        }
        virtual void Stop() override
        {
            //printf("Shutting down...\n");
            if (my_node_.role == Node::SERVER)
            {
                //printf("PSHUB Shutting down...\n");
                stopRequested = true;
                for_each(threads.begin(), threads.end(), [](std::thread& x) {x.join(); });
            }
            InfiniBandVan::Stop();
            //printf("Infiniband Shutting Down...\n");
        }

        virtual std::string VanType() override
        {
            return "pshub";
        }

        //must unify all these Smmarize calls
        std::string Summarize(std::vector<float>& v, int firstN = 10)
        {
            auto ptr = (float*)v.data();
            auto sz = v.size();
            auto sigptr = (int*)ptr;
            int result = 0;
            std::stringstream ss;
            ss << "Vector Summary:(";
            for (size_t i = 0; i < firstN && i < sz; i++)
            {
                ss << ptr[i] << ",";
            }
            ss << ")";
            for (size_t i = 0; i < sz; i++)
            {
                result ^= sigptr[i];
            }
            ss << " Signature:" << result;
            return ss.str();
        }

        void PerCoreNUMAAwareInitialization(size_t tid)
        {

            //first, everyone need to setup their QP Counters.
            //figure out what QPs belong to me.
            std::vector<int> myQPs;
            for (int qpIdx = 0; qpIdx < verbs->endpoints.size(); qpIdx++)
            {
                if (verbs->endpoints.at(qpIdx).CoreIdx == tid)
                {
                    myQPs.push_back(qpIdx);
                }
            }
            if (myQPs.size() == 0) return;
            //create a buffer large enough to hold all counters.
            //this buffer must from our socket.
            auto mySock = verbs->Core2SocketIdx.at(tid);
            CHECK(mySock == verbs->endpoints.at(myQPs.back()).SocketIdx);
            //create a large, Cache-Aligned buffer on my socket for my QPStat counters.
	    //There're only a few QPs. Do not use 2MB alignment
            int* arr = (int*)AlignedAllocateUniversal(RoundUp(myQPs.size() * sizeof(int), CACHELINE_SIZE_BYTES), mySock, CACHELINE_SIZE_BYTES);
	    CHECK(arr!=NULL) << "Cannot allocate queue pair counter on numa node " << mySock << ". Reduce queue pairs?";
            //put it in QPStats.
            for (int i = 0; i < myQPs.size(); i++)
            {
                verbs->QPStats.at(myQPs.at(i)) = arr + i;
            }
            if (verbs->DirectConnect == false)
            {
                //now deal with counetrs. Counters should also be packed per core.
                std::vector<int> myKeys;
                for (int i = 0; i < keySize.size();i++)
                {
                    //is this my key?
                    auto wQP = verbs->workerKey2QPIdx[i];
                    auto target = verbs->workerQP2SrvId[wQP];
                    if (target == ps::Postoffice::Get()->my_rank())
                    {
                        //put 0 there because the key determines the core in FreeConnect mode.
                        if (verbs->Helper_Server_GetEndpointFromKey(i, 0).CoreIdx == tid)
                        {
                            myKeys.push_back(i);
                        }
                    }
                }
                //allocate many atomic counters.
                atomic_int* atomInts = (atomic_int*)AlignedAllocateUniversal(RoundUp(myKeys.size() * sizeof(atomic_int), CACHELINE_SIZE_BYTES), mySock);
		CHECK(atomInts != NULL) << "Cannot allocate atomic int counters for keys on numa node " << mySock << ". Reduce key chunk?";
                //now put the pointers back into the array for easy access.
                for (int i = 0; i < myKeys.size(); i++)
                {
                    UpdatesReceivedBuffer.at(myKeys.at(i)) = atomInts + i;
                }
            }
        }
	volatile bool GTG = false;
        void infiniBandCollect(size_t tid)
        {
	    while(GTG == false);
            //check my Numa assignment is in sane.
            CHECK(GetCurrentThreadNumaNode() == verbs->Core2SocketIdx.at(tid)) << "Current node = " << GetCurrentThreadNumaNode() << " expected = " << verbs->Core2SocketIdx.at(tid);
            CHECK(my_node_.role == Node::SERVER);//sanity check
            auto pollSize = verbs->core2CQIdxs.at(tid).size();
            if (pollSize == 0) return;
            PerCoreNUMAAwareInitialization(tid);
            //printf("[%d][%d]PSHUB data thread %d is up.\n", my_node_.id, my_node_.role, tid);
            auto workers = ps::Postoffice::Get()->GetNodeIDs(kWorkerGroup);
            uint64_t memoizedPoll = 0;
	    //create a local copy to avoid verbs floating around.
	    std::vector<int> pollVector = verbs->core2CQIdxs.at(tid);
            while (stopRequested == false)
            {
                ibv_wc wc;
                size_t cqIdx = pollVector.at(memoizedPoll++ % pollSize);
                if (0 == verbs->poll(1, cqIdx, Verbs::CompletionQueueType::Receive, &wc))
                {
                    if (stopRequested) return;
                    continue;
                }

                //sleep(0);
                        //poll the infiniband connection and see whats going on.
                        //pull key j.
                        //printf("[%d]Polling Completion Queue id = %d\n",msg->meta.recver,j);
                auto sender = (wc.imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK;
                auto mit = node2MachineIdx.at(sender);
                CHECK(mit != -1) << " no route to " << sender;
                auto i = mit;
                auto j = wc.imm_data & PHUB_IMM_KEY_MASK;
                //printf("[%d][PRE]PSHUB received key=%d from=%d len=%d more=%d, updatesreceived = %d, workersize = %d\n", dbgMyId, j, sender, wc.byte_len, UpdatesReceivedBuffer[j].core.load() - workers.size(), UpdatesReceivedBuffer[j].core.load(), workers.size());

                        //printf("[%d][%d]Posting RecvBuffer[%d][%d]. Msg from %d. Size=%d\n",dbgMyId, my_node().role,i,j,wc.imm_data, wc.byte_len);
                ReceiveRequests[wc.wr_id]->PostReceiveRequest();
                //printf("[%d][%d]RECVDBG byte_len=%d\n", dbgMyId, my_node_.role,wc.byte_len);
                auto& buffer = psRecvBuffer[i][j];
                MetaSlim* ms = buffer.pMetaSlimBuffer;
                //CHECK(ms->simple_app == false);

                //CHECK(ms->sender == sender) << "meta sender = " << ms->sender << " ibSender " << sender << " "<<ms->DebugString(); in theory we should check but it is not always populated by mxnet
                //PSHUB Does not allow explicit PULL requests.
                //see PHUB Pull Elision
                //CHECK(ms->push == true);
                /*if(j == 0 && keySize[j] / sizeof(float) > 60000)
                {
                printf("[PUSH0] %s\n", SummarizeContinousBuffer((float*)recvBuffer[i][j].KVBuffer, 60000).c_str());
                printf("[PUSH1] %s\n", SummarizeContinousBuffer((float*)recvBuffer[i][j].KVBuffer + 60000, keySize[0] / sizeof(float)- 60000).c_str());
                }
                else if((j == 0 || j == 1) && keySize[0] / sizeof(float) == 60000)
                {
                printf("[PUSH%d] %s\n",j, SummarizeContinousBuffer((float*)recvBuffer[i][j].KVBuffer, keySize[j] / sizeof(float)).c_str());
                }*/
                //PULL
                MEASURE_THIS_TIME_BEG_EX(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL, j, ms->PHubAdditionalPayload);
                MEASURE_THIS_TIME_END(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL, j);
                //i need to aggregate all elements.
                 //turn off checks
                 /*if(verbs->VerbsBitwidthReductionRatio == 1)
                 {
                 CHECK(wc.byte_len == buffer.KVLength + sizeof(MetaSlim))
                             << wc.byte_len
                             << " vs "
                             << (buffer.KVLength + sizeof(MetaSlim));
                 CHECK(keySize[j] == buffer.KVLength);

                 }*/
                 //SArray<float> dbgData = SArray<float>(SArray<char>(buffer + sizeof(MetaSlim), keySize[j]));
                 //printf("[PSHub][DataDelivery]Key=%d, Summary=%s\n", j, dbgData.Summarize(10).c_str());
                /* if(j == 0) */
                /*   { */
                /*     printf("[BEFORE]received= %s\n", SummarizeContinousBuffer((float*)buffer.pKVBuffer, keySize[0]/sizeof(float)).c_str()); */
                /*   } */

                auto sid = verbs->Helper_Server_GetEndpointFromKey(j, i).SocketIdx;
                if (SuppressAggregator == false)
                {
                    MEASURE_THIS_TIME_BEG(PERFMON_PHUB_KEY_MERGE, j);
                    //this is fine because in a socket only one processor can see this key.
                    //so even though all of them sees 0, they can copy no problem.
                    if (UpdatesReceivedBuffer.at(j)->load() == 0)
                    {
                        //copy. in fact we can do this for each socket, but unfortunately UpdatesReceivedBuffer is per key, not per socket per key.
                        memcpy(PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(), buffer.pKVBuffer, keySize[j]);
                    }
                    else
                    {
                        //CHECK(neata.size() == keySize[j] / sizeof(float)) << "newData = "<<newData.size() << " bytes vs " << keySize[j] / sizeof(float);
                        //CHECK(newData.size() == MergeBuffers[j].size());
                        auto mergeBuffer = (float*)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer();
                        //lets play with SSE for a bit.

                        ADD->VectorVectorAdd((float*)mergeBuffer, PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE, (float*)buffer.pKVBuffer);
                    }

                    MEASURE_THIS_TIME_END(PERFMON_PHUB_KEY_MERGE, j);
                }
                //this one has payload
        //this cannot be done alone. use rmw to test for value.
                //UpdatesReceivedBuffer[j].core++;
                //printf("[%d]PSHUB received key=%d from=%d len=%d more=%d, updatesreceived = %d, workersize = %d\n", dbgMyId, j, sender, wc.byte_len, UpdatesReceivedBuffer[j].core.load() - workers.size(), UpdatesReceivedBuffer[j].core.load(), workers.size());
                //to enable asynchronous mode, comment out the if statement.
                if (IsAsync || UpdatesReceivedBuffer[j]->fetch_add(1, std::memory_order_relaxed) + 1 == workers.size())
                {
                    //DirectConnect requires reset this counter BEFORE the first BundledPush/Pull ack is sent out.
                    //So that no signal is lost.
                    UpdatesReceivedBuffer[j]->store(0, std::memory_order_relaxed);//bye bye. safe because im still listening to the key

                            //initiate fast ack.
                            //before doing anything else, flip the write/pull buffer indicator, so fast pushers dont 
                            //stand in the way while some remaining workers are still pulling.
                            //let me first copy all other socket's buffer to mine.
                    //switch connected workers enforces a key only goes into one socket.
                    if (IsAsync == false && verbs->DirectConnect == true)
                    {
                        //let async be out of sync across sockets.
                        for (size_t socket = 0; socket < verbs->SocketCount; socket++)
                        {
                            if (socket != sid)
                            {
                                ADD->VectorVectorAdd((float*)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(),
                                    PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE,
                                    (float*)PerSocketMergeBuffers[socket][j].GetCurrentWriteBuffer());
                                //ADD->VectorVectorAdd((float*)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(),
                                //    PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE,
                                //    (float*)PerSocketMergeBuffers[socket][j].GetCurrentWriteBuffer());
                            }
                        }
                    }
                    //Do optimization here.
                    if (OPT != NULL && SuppressOptimizer == false)
                    {
                        /*if(j == 0 && keySize[j] / sizeof(float) > 60000)
                        {
                        printf("[AGG0] %s\n", SummarizeContinousBuffer((float*)MergeBuffers[0].GetCurrentReadBuffer(), 60000).c_str());
                        printf("[AGG1] %s\n", SummarizeContinousBuffer((float*)MergeBuffers[0].GetCurrentReadBuffer() + 60000, keySize[0] / sizeof(float)-60000).c_str());
                        }
                        else if((j == 0 || j == 1) && keySize[0] / sizeof(float) == 60000)
                        {
                        printf("[AGG%d] %s\n",j, SummarizeContinousBuffer((float*)MergeBuffers[j].GetCurrentReadBuffer(), keySize[j] / sizeof(float)).c_str());
                        }*/

                        /* if(j == 0) */
                        /*   { */
                        /*     printf("[BEFORE]merged= %s\n", SummarizeContinousBuffer((float*)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(), keySize[0]/sizeof(float)).c_str()); */
                        /*     printf("[BEFORE]model= %s\n", SummarizeContinousBuffer((float*)PerSocketMergeBuffers[sid][j].GetCurrentReadBuffer(), keySize[0]/sizeof(float)).c_str()); */
                        /*   } */

                          //printf("[BEFORE]sending to optimize key = %d. Agg=%p, Weight=%p, len=%d. %s\n",j, MergeBuffers[j].GetCurrentWriteBuffer(), MergeBuffers[j].GetCurrentReadBuffer(), MergeBuffers[j].ActualElementCountPaddedForSSE, SummarizeContinousBuffer((float*)MergeBuffers[j].GetCurrentReadBuffer(), keySize[j]/sizeof(float)).c_str());
                          //if an updater is present, we do not need to flip the buffer, as reader buffer is always the place to pull from 
                          //and write buffer is always the place to merge into.

                        OPT->Update(j,
                            (float*)PerSocketMergeBuffers[sid][j].GetCurrentReadBuffer(),
                            (float*)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(),
                            PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE);
                        /*if(j == 0 && keySize[j] / sizeof(float) > 60000)
                        {
                        printf("[OPT0] %s\n", SummarizeContinousBuffer((float*)MergeBuffers[0].GetCurrentReadBuffer(), 60000).c_str());
                        printf("[OPT1] %s\n", SummarizeContinousBuffer((float*)MergeBuffers[0].GetCurrentReadBuffer() + 60000, keySize[0] / sizeof(float)-60000).c_str());
                        }
                        else if((j == 0 || j == 1) && keySize[0] / sizeof(float) == 60000)
                        {
                        printf("[OPT%d] %s\n",j, SummarizeContinousBuffer((float*)MergeBuffers[j].GetCurrentReadBuffer(), keySize[j] / sizeof(float)).c_str());
                        }
                        */

                        //printf("[AFTER]sending to optimize key = %d. Agg=%p, Weight=%p, len=%d. %s\n",j, MergeBuffers[j].GetCurrentWriteBuffer(), MergeBuffers[j].GetCurrentReadBuffer(), MergeBuffers[j].ActualElementCountPaddedForSSE, SummarizeContinousBuffer((float*)MergeBuffers[j].GetCurrentReadBuffer(), keySize[j]/sizeof(float)).c_str());

                    }
                    else
                    {
                        //This just flips the merge read/write buffer.
                        //do not care what this means.
                        if (IsAsync == false)
                        {
                            for (size_t socket = 0; socket < verbs->SocketCount; socket++)
                            {
                                PerSocketMergeBuffers[socket][j].AggregationAndOptimizationReady();
                            }
                        }
                        else
                        {
                            PerSocketMergeBuffers[sid][j].AggregationAndOptimizationReady();

                        }

                    }
                    //we have flipped the buffer. We're clear to send out fast acks.
                    //we dont need to clear up timestamps before pushing, because this thread is in control.
                    if (IsAsync == false)
                    {
                        //copy back to each socket.
                        if (verbs->DirectConnect == true)
                        {
                            for (size_t socket = 0; socket < verbs->SocketCount; socket++)
                            {
                                if (socket != sid)
                                {
                                    //printf("copying final buffer from %d to %d\n", sid, socket);
                                    memcpy((float*)PerSocketMergeBuffers[socket][j].GetCurrentReadBuffer(),
                                        (float*)PerSocketMergeBuffers[sid][j].GetCurrentReadBuffer(),
                                        PerSocketMergeBuffers[sid][j].ActualElementCountPaddedForSSE);
                                }

                            }
                        }
                        for (auto it = 0; it < workers.size(); it++)
                        {
                            //auto it = node2MachineIdx[id];
                //CHECK(it != -1) << " No route to " << id << " from " << my_node_.id;
                            //MetaSlim* meta = psSendBuffer[it][j].pMetaSlimBuffer;
                            //printf("[PSHuB] sending out key = %d to %d, tid = %d\n", j, it*2+9, tid);
                            //meta->push = true;
                            //meta->request = false;
                            //meta->sender = my_node_.id;
                            //meta->recver = id;
                            //meta->simple_app = false;
                            ////PHuB/mxnet is the only customer.
                            ////the following assignments are not very useful.
                            //meta->customer_id = 0;
                            //meta->head = 0;
                            //find the right socketId.
                            auto socketId = verbs->Helper_Server_GetEndpointFromKey(j, it).SocketIdx;
                            //elided pulls
                            PerSocketMergeBuffers[socketId][j].PostSendBufferBundledPushPullAck(it);
                            //psSendBuffer[it][j]
                            /* if(it == 0 && j == 0) */
                            /*   { */
                            //printf("[AFTER]merged= %s\n", SummarizeContinousBuffer((float*)PerSocketMergeBuffers[sid][j].GetCurrentWriteBuffer(), keySize[0]/sizeof(float)).c_str()); */
                            /*     printf("[AFTER]model= %s\n", SummarizeContinousBuffer((float*)PerSocketMergeBuffers[sid][j].GetCurrentReadBuffer(), keySize[0]/sizeof(float)).c_str()); */
                            /*   } */
                        }
                    }
                    else
                    {
                        //MetaSlim* meta = psSendBuffer[i][j].pMetaSlimBuffer;
                        //meta->push = true;
                        //meta->request = false;
                        //meta->recver = meta->sender;
                        //meta->sender = my_node_.id;
                        auto socketId = verbs->Helper_Server_GetEndpointFromKey(j, i).SocketIdx;
                        //return just one, not all.
                        PerSocketMergeBuffers[socketId][j].PostSendBufferBundledPushPullAck(i);
                    }
                }
                //printf("[%d][post]PSHUB received key=%d from=%d len=%d more=%d, updatesreceived = %d, workersize = %d\n", dbgMyId, j, sender, wc.byte_len, UpdatesReceivedBuffer[j].core.load() - workers.size(), UpdatesReceivedBuffer[j].core.load(), workers.size());

            }
            printf("[%d][%d]PSHUB data thread shutting down\n", my_node_.id, my_node_.role);
        }

    protected:
        virtual int GetUnderlyingWorkerThreadCount() override
        {
            if (my_node_.role != Node::SERVER)
            {
                return 1;
            }
            else
            {
                //if server 
                return MaximumAllowedThreads;
            }
        }
    };
}
