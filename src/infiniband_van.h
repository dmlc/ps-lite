#pragma once
#include <stdlib.h>
#include <thread>
#include <string>
#include <ps/internal/van.h>
#include <ps/internal/postoffice.h>
#include "dmlc/DIME.h"
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <unordered_set>
#include <mutex>
#include "zmq_van.h"
#include <thread>
#include <unistd.h>
#include <signal.h>
#include "PHUB.h"
#include "lfq.h"
#include "Verbs.hpp"
namespace ps
{
    class InfiniBandVan :public ZMQVan
    {
    public:
        InfiniBandVan()
        {
            FeatureSet = Van::WorkerSidePushPullZeroCopy | Van::RequiresSendBufferKeyCopyAsServer | Van::SupportsKeyChunking | Van::NativeInfiniband | Van::SupportsBitwidthSIM;
            auto qpcnt = Environment::Get()->find("IB_QP_COUNT");
            if (qpcnt != NULL)
            {
                QPCountOverride = atoi(qpcnt);
            }
            auto dConn = Environment::Get()->find("IB_DIRECT_CONNECT");
            if (dConn != NULL)
            {
                DirectConnect = true;
            }

        }
        Verbs* verbs = NULL;
        bool DirectConnect = false;
        //These two actually alias to the same underlying address. Used because it's clearer in code.
        WorkerIBBuffer** wSendBuffer = NULL;
        WorkerIBBuffer** wRecvBuffer = NULL;

        //These two actually alias to the same underlying address. Used because it's clearer in code.
        PSIBBuffer** psSendBuffer = NULL;
        PSIBBuffer** psRecvBuffer = NULL;

        //The receive request buffers.
        //These are used instead of mangling them with the send/recvBuffers to accomodate RDMA.
        std::vector<IBReceiveRequest*> ReceiveRequests;

        int machineCount;
        int receivedQPs = 0;
        //maps an MXNET ID to a machine index used to access buffers.
        std::vector<int> node2MachineIdx;
        //for debugging purposes only
        int dbgMyId;
        int QPCountOverride = -1;
        //Terrible, but okay for now.
        std::vector<ps::SArray<uintptr_t>> remoteKVAddrs;
        std::vector<ps::SArray<uintptr_t>> remoteMetaAddrs;

        //PHub::LockFree::PHubLFIntegerQueue* pPHubWorkerSidePullQueue;
        virtual int PHUBGetKeySharding(Key key) override
        {
            CHECK(verbs != NULL);
            auto qpidx = verbs->workerKey2QPIdx.at(key);
            return verbs->workerQP2SrvId.at(qpidx);
            //THIS MUST BE CONSISTENT WITH THE ASSIGNMENT SCHEME IN VERB.CC::INITIALIZE_QUEUE_PAIR
            /*if (QPCountOverride > 0)
            {
                return (key % QPCountOverride) % ps::Postoffice::Get()->num_servers();
            }
            else
            {
                CHECK(keySize.size() > 0);
                return key % ps::Postoffice::Get()->num_servers();
        }*/
        }

        virtual void OnKeyPopulated() override
        {
            dbgMyId = my_node_.id;
            //printf("[%d][%d]Key Population Finished. %d keys registered.\n", my_node_.id, my_node_.role, keySize.size());
            PS_VLOG(1) << "node " << my_node_.id << "(" << my_node_.role << ") received " << keySize.size() << " keys";
            if (my_node().role == ps::Node::Role::SCHEDULER) return;//nothing to do.
            int keyCount = keySize.size();
            machineCount = my_node_.role == ps::Node::Role::SERVER ? Postoffice::Get()->num_workers() : Postoffice::Get()->num_servers();
            //create some extra rooms because server and worker counts do not need to be equal and worker / server id skips
            int maxCnt = std::max(ps::Postoffice::Get()->num_workers(), ps::Postoffice::Get()->num_servers());
            node2MachineIdx.resize(8 + 2 * maxCnt, -1);

            remoteKVAddrs.resize(machineCount);
            remoteMetaAddrs.resize(machineCount);
            //initialize pointers.
            if (my_node_.role == ps::Node::Role::SERVER)
            {
                psSendBuffer = new PSIBBuffer*[machineCount];
                psRecvBuffer = psSendBuffer;
                //initialize each actual buffers.
                for (int i = 0; i < machineCount; i++)
                {
                    psSendBuffer[i] = new PSIBBuffer[keySize.size()];
                }

            }
            else
            {
                wSendBuffer = new WorkerIBBuffer*[machineCount];
                wRecvBuffer = wSendBuffer;
                for (int i = 0; i < machineCount; i++)
                {
                    wSendBuffer[i] = new WorkerIBBuffer[keySize.size()];
                }
            }

            //remember, the param is the number of QPairs a WORKER establishes.
            int qpCnt = QPCountOverride > 0 ? QPCountOverride : (int)keyCount;
            //no prefered interface.
        //check in kv_store.dist.h
            if (my_node_.role == Node::SERVER)
            {
                CHECK(verbs == NULL);
                verbs = new Verbs(qpCnt, GetUnderlyingWorkerThreadCount(), my_node().id, keySize, DirectConnect, "");
            }
            else
            {
                CHECK(verbs != NULL);
            }

            auto allocator = PHubAllocator::Get();
            if (allocator->IsInitialized())
            {
                CHECK(my_node_.role == Node::WORKER);
                //workers MUST have initialized this in kvdist_server.h to enable 0 copy
            }
            else
            {
                //a pshub configuration.
                //CHECK(VanType() == "pshub" || VanType() == "infiniband");
                CHECK(verbs->SocketCount > 0 && machineCount > 0);
                //the last argument is ignored. this is a server.
                allocator->Init(keySize, true, machineCount, sizeof(MetaSlim), verbs->SocketCount, true);
            }
            //Conenct QUeue pairs requires Remote key, which requires registration, which requires verbs to be initialized.
            //this is the only correct order of these tasks
            ps::Postoffice::Get()->Barrier(kWorkerGroup + kServerGroup);
            dbgMyId = my_node().id;
            CHECK(verbs != NULL);
            printf("[%d]Preparing Lid and QP exchanges. Worker QPCountOverride = %d\n", my_node_.id, QPCountOverride);
            verbs->connect_queue_pairs();
            if ((QPCountOverride != -1))
            {
                printf("%s\n", verbs->Summarize(std::to_string(my_node_.id), my_node_.id == 8, false).c_str());
            }
            else
            {
                //default, no locking is enabled.
                verbs->SmartPostNoLocking = true;
            }
            printf("[%d]Infiniband Verbs Initialization Finished\n", my_node_.id);
            //printf("[%d]infiniband Setup\n",my_node_.id);
            //foreach key register
            int initCount = 0;
            auto expectedInitCnt = (my_node_.role == Node::WORKER ? keySize.size() : machineCount * verbs->serverKeyCounts[ps::Postoffice::Get()->IDtoRank(my_node_.id)]);
            for (size_t i = 0; i < machineCount; i++)
            {
                for (int j = 0; j < keyCount; j++)
                {
                    int size = keySize[j];// +sizeof(MetaSlim);
            //cannot just blindly divide.
            //only reduce size if after reduction there is at least one float.
                    auto qpIdx = verbs->workerKey2QPIdx[j];
                    //only initialize if i am talking to machine i about this key.
            //CHECK(vKeyRecvAddress.size() == keySize.size()) <<"[" << my_node_.id << "] vkeyRecvAddress.size() = " << vKeyRecvAddress.size() << " vs " << keySize.size();
                    if (my_node_.role == Node::WORKER && verbs->workerQP2SrvId[qpIdx] == i)
                    {
                        //check if this key target at this server (i).
                        initCount++;
                        CHECK(wRecvBuffer[i][j].initialized == false) << "Duplicate key mapping?  Key = " << j << " worker = " << my_node_.id;
                        auto entry = vKeyAddress.find(j);
                        CHECK(entry != vKeyAddress.end());
                        auto entryRecv = vKeyRecvAddress.find(j);
                        CHECK(entryRecv != vKeyRecvAddress.end());
                        //CHECK(entry->second != 0) <<my_node_.role << " Key " << j << " Wants to register NULL.";
            //printf("worker key = %d, workerKey2QPIdx.size() = %d i = %d\n", j, verbs->workerKey2QPIdx.size(), i);

                        Verbs::Endpoint& associatedEP = verbs->Helper_Worker_GetEndpointFromKey(j);
                        ibv_mr* memRegion = verbs->DeviceMemoryRegions.at(associatedEP.DeviceIdx);
                        CHECK(memRegion != NULL);
                        size_t len = 0;
                        //two reasons: allocator keeps track of PHYSICAL keys, while these entries keep track of virtual keys.
                        //allocator len shows padded len - may be longer than actual key
                        CHECK(entryRecv->second == (uint64_t)allocator->WorkerKVBuffer(j, associatedEP.SocketIdx, len)) << entryRecv->second << " v.s " << (uint64_t)allocator->WorkerKVBuffer(j, associatedEP.SocketIdx, len);
                        CHECK(entry->second == (uint64_t)allocator->WorkerKVBuffer(j, associatedEP.SocketIdx, len));
                        //for worker, these two buffers are now combined to eliminate ALL copies.
            //printf("key = %d, recvBuffer = %llu, sendBuffer = %llu\n", j,entryRecv->second, entry->second);
                        CHECK((uint64_t)remoteMetaAddrs[i][j] + sizeof(MetaSlim) == (uint64_t)remoteKVAddrs[i][j]);
                        wSendBuffer[i][j].Init(verbs,
                            i,
                            size,
                            j,
                            false,
                            (char*)entryRecv->second,
                            (MetaSlim*)allocator->WorkerMetaBuffer(j, associatedEP.SocketIdx, len),
                            (char*)remoteMetaAddrs[i][j],
                            memRegion);
                        //2nd param is 0 because there is only 1 copy
                        allocator->VerifyWorkerKV(associatedEP.SocketIdx, 0, j, wSendBuffer[i][j].pKVBuffer, (void*)wSendBuffer[i][j].pKVBuffer + wSendBuffer[i][j].ActualKVSizePaddedForSSE);
                        allocator->VerifyWorkerMeta(associatedEP.SocketIdx, 0, j, wSendBuffer[i][j].pMetaSlimBuffer, wSendBuffer[i][j].pMetaSlimBuffer + 1);

                        CHECK(wRecvBuffer[i][j].initialized);
                        //only difference to these buffer is which q it belongs to.
                        CHECK(wRecvBuffer[i][j].QPIndex == associatedEP.Index);
                        ReceiveRequests.push_back(new IBReceiveRequest(wRecvBuffer[i][j].QPIndex, verbs, ReceiveRequests.size()));
                        //printf("%s\n", wRecvBuffer[i][j].Summarize().c_str());
                        //printf("key = %d, buf = %p - %p, meta = %p - %p\n", j, (char*)entryRecv->second, (char*)entryRecv->second + keySize[j], allocator->WorkerMetaBuffer(j,len), allocator->WorkerMetaBuffer(j,len) + 1);
                    }
                    else if (my_node_.role == Node::SERVER && verbs->workerQP2SrvId[qpIdx] == ps::Postoffice::Get()->IDtoRank(my_node_.id))
                    {
                        initCount++;
                        //do i care about this key?
                        size_t len = 0;
                        //this is a server. only recvBuffer is required. send buffer is a merge buffer.
                        //these two point to exact buffers. alias them for easier access.
                        //allocator uses virtual keys.
                        /*for(auto ptr : remoteKeyAddrs[i])
                          {
                          printf("[%d] remoteKeyAddrs[%d] = %p\n", my_node_.id, i, ptr);
                          }*/
                        Verbs::Endpoint& associatedEP = verbs->Helper_Server_GetEndpointFromKey(j, i);
                        CHECK(allocator->PHUBReceiveKVBuffer(j, i, associatedEP.SocketIdx, len) == allocator->PHUBReceiveMetaBuffer(j, i, associatedEP.SocketIdx, len) + sizeof(MetaSlim));
                        //printf("[server]key = %d recv buf = %llu, meta = %llu, remote meta = %llu, remote recv = %llu\n", j, allocator->PHUBReceiveKVBuffer(j, i, associatedEP.SocketIdx, len), allocator->PHUBReceiveMetaBuffer(j,i,associatedEP.SocketIdx,len), remoteMetaAddrs[i][j], remoteKVAddrs[i][j]);
                        psSendBuffer[i][j].Init(verbs,
                            i,
                            size,
                            j,
                            true,
                            (char*)allocator->PHUBReceiveKVBuffer(j, i, associatedEP.SocketIdx, len),
                            (MetaSlim*)allocator->PHUBReceiveMetaBuffer(j, i, associatedEP.SocketIdx, len),
                            (char*)remoteKVAddrs[i][j],
                            (MetaSlim*)remoteMetaAddrs[i][j],
                            verbs->DeviceMemoryRegions.at(associatedEP.DeviceIdx));
                        CHECK(psRecvBuffer[i][j].initialized);
                        allocator->VerifyPHUBRecvKV(associatedEP.SocketIdx, i, j, psSendBuffer[i][j].pKVBuffer, (void*)psSendBuffer[i][j].pKVBuffer + psSendBuffer[i][j].ActualKVSizePaddedForSSE);
                        allocator->VerifyPHUBRecvMeta(associatedEP.SocketIdx, i, j, psSendBuffer[i][j].pMetaSlimBuffer, psSendBuffer[i][j].pMetaSlimBuffer + 1);
                        if (VanType() != "pshub")
                        {
                            ReceiveRequests.push_back(new IBReceiveRequest(psRecvBuffer[i][j].QPIndex, verbs, ReceiveRequests.size()));
                        }
                        else
                        {
                            //PHUB uses Migration to mitigate XS Sock traffic.
                            //MigrateToNumaNode(associatedEP.SocketIdx);
			    IBReceiveRequest * loc = (IBReceiveRequest*) AlignedAllocateUniversal(sizeof(IBReceiveRequest), associatedEP.SocketIdx, 64);
			    //auto loc = new /*(buf)*/ IBReceiveRequest(psRecvBuffer[i][j].QPIndex, verbs, ReceiveRequests.size());
			    new (loc) IBReceiveRequest(psRecvBuffer[i][j].QPIndex, verbs, ReceiveRequests.size());
                            ReceiveRequests.push_back(loc);
                        }
                        CHECK(psRecvBuffer[i][j].QPIndex == associatedEP.Index);

                        //psRecvBuffer[i][j].PostReceiveBuffer();
                        //printf("recv dbg %s\n", psRecvBuffer[i][j].Summarize().c_str());
                    }
                    if (VanType() != "pshub")
                    {
                        CHECK(verbs->core2CQIdxs.size() == 1);
                        //CHECK(verbs->core2CQIdxs[0].size() == 1);
                    }
                    //register these
                    //printf("[%d][%d]RecvBuffer Summary:\n",dbgMyId,my_node_.role);
                    //recvBuffer[i][j].Summarize();
                    //printf("[%d][%d]SendBuffer Summary:\n",dbgMyId,my_node_.role);
                    //sendBuffer[i][j].Summarize();
                }
            }

            //now post receive request buffers.
            for (auto rr : ReceiveRequests)
            {
		
                rr->PostReceiveRequest();
            }

            //ReceiveRequests.resize(expectedInitCnt);//i need these many receive requests.

            CHECK(initCount == expectedInitCnt) << "[" << my_node_.id << "] mismatched initialization. init = " << initCount << " expected count = " << expectedInitCnt;
            //printf("[%d]recvBuffer Setup\n",my_node_.id);
            //try to connect.
            //need to post these buffer now.

            //everyone has posted.
            verbs->CheckMapIntegrity(keySize.size());
            ps::Postoffice::Get()->Barrier(kWorkerGroup + kServerGroup);
            //pPHubWorkerSidePullQueue = new PHub::LockFree::PHubLFIntegerQueue(keySize.size(), true);
            dataThroughInfiniband = true;

            LOG(INFO) << "data is now transmitted through infiniband\n";

        }
        bool dataThroughInfiniband = false;
        //must be called by the thread other than receving thread

        virtual void WaitForQPExchanges(int count) override
        {
            while (receivedQPs != count)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        //thread unsafe. must be called by the receiving thread.
        virtual void HandleSpecialCommand(Message& msg) override
        {
            //  PS_VLOG(1)<<"node "<<my_node().id<< " received INFINIBAND SPECIAL CMD " << msg.DebugString();

            //let's override this
            auto origin = msg.meta.sender;
            auto role = my_node_.role;
            //printf("[%d] receives command message %s\n", my_node_.id, msg.DebugString().c_str());
            if (msg.meta.control.cmd == Control::INFINIBANDXCHGQPS)
            {
                //auto qpCnt = verbs->serverQPSizes[origin];
               // msg.AddData(SArray<uint32_t>(myQPBundles));
               // msg.AddData(SArray<uint16_t>(myLids));
               // msg.AddData(SArray<uint32_t>(myRKeys));
               // msg.AddData(SArray<uintptr_t>(myKeyAddrs));
               // msg.AddData(SArray<uintptr_t>(myMetaAddrs));
                CHECK(node2MachineIdx[origin] == -1) << " InvalidState: origin = " << origin << " node2MachineIdx[origin] = " << node2MachineIdx[origin];
                node2MachineIdx[origin] = ps::Postoffice::Get()->IDtoRank(origin);
                auto machineIndex = node2MachineIdx[origin];
                //printf("[%d] assigns origin = %d to idx = %d\n", my_node_.id, origin, machineIndex);
                auto qpCnt = role == Node::SERVER ? verbs->serverQPCounts[ps::Postoffice::Get()->IDtoRank(my_node().id)] : verbs->serverQPCounts.at(machineIndex);
                CHECK(msg.data.size() == 5);
                auto cnt = msg.meta.control.msg_sig;
                CHECK_EQ(cnt, qpCnt) << "[" << my_node_.id << "] expected wrong key size! " << cnt << " received " << qpCnt << " expected.";
                auto start = machineIndex * qpCnt;
                SArray<uint32_t> qps = SArray<uint32_t>(msg.data[0]);
                SArray<uint16_t> lids = SArray<uint16_t>(msg.data[1]);
                SArray<uint32_t> rkeys = SArray<uint32_t>(msg.data[2]);
                CHECK(qps.size() == cnt);
                int myRank = ps::Postoffice::Get()->my_rank();
                //first, extract Qpairs.
                for (size_t i = 0; i < qpCnt; i++)
                {
                    //this is remote.
                    if (my_node_.role == Node::SERVER)
                    {
                        CHECK(start + i < verbs->endpoints.size());
                        verbs->endpoints[start + i].qp_num = qps[i];
                        verbs->endpoints[start + i].lid = lids[i];
                        verbs->endpoints[start + i].RemoteKey = rkeys[i];
                        verbs->endpoints[start + i].RemoteQPIdx = verbs->serverIDQPIdx2WkrQPIdx.at(myRank).at(i);
                    }
                    else
                    {
                        //if this is a worker. I need to rearrange the keys.
                        //map these qps back with the maps i have.
                        auto wqidx = verbs->serverIDQPIdx2WkrQPIdx[machineIndex][i];
                        verbs->endpoints[wqidx].qp_num = qps[i];
                        verbs->endpoints[wqidx].lid = lids[i];
                        verbs->endpoints[wqidx].RemoteKey = rkeys[i];
                        verbs->endpoints[wqidx].RemoteQPIdx = i + myRank * verbs->serverQPCounts.at(machineIndex);
                    }
                    //printf("[%d]received qps[%d] from %d with value %d\n",my_node().id,start+i, origin,qps[i]);
                }
                //then, extract key and meta addresses.
                //potential leak of these two messages.
        //each key needs to report their location.
                remoteKVAddrs[machineIndex] = SArray<uintptr_t>(msg.data[3]);
                /*for(size_t i = 0; i < remoteKVAddrs[machineIndex].size(); i++)
                {
                    printf("[%d] received remote key = %d addr = %llu\n", my_node_.id, i, remoteKVAddrs[machineIndex][i]);
                    }*/

                remoteMetaAddrs[machineIndex] = SArray<uintptr_t>(msg.data[4]);
                CHECK(remoteKVAddrs[machineIndex].size() == keySize.size()) << " incorrect number of remote key addresses receievd " << remoteKVAddrs[machineIndex].size() << " v.s. " << keySize.size();
                CHECK(remoteMetaAddrs[machineIndex].size() == keySize.size()) << " incorrect number of remote meta addresses received " << remoteMetaAddrs[machineIndex].size() << " v.s. " << keySize.size();
                receivedQPs++;
            }
            else if (msg.meta.control.cmd == Control::INFINIBANDKEYSIZEEXCHG)
            {
                CHECK(msg.data.size() > 0);
                SArray<Key> keys = SArray<Key>(msg.data[0]);
                CHECK(keys.size() == 1);
                CHECK(keys[0] == -1);
                SArray<int> sizes = SArray<int>(msg.data[1]);
                for (size_t i = 0; i < sizes.size(); i++)
                {
                    SetKeySize(i, (int)sizes[i] * sizeof(float));
                    //printf("[INFINIBAND][SERVER] key %d, Sz %d received.\n", i, sizes[i]);
                }
            }
            //  PS_VLOG(1)<<"node "<<my_node().id<< " PROCESSED INFINIBAND SPECIAL CMD " << msg.DebugString();
            //verbs->Summarize();

        }

        virtual void Stop() override
        {
            ZMQVan::Stop();
            if (verbs != NULL)
            {
                verbs->finalize();
                delete verbs;
            }
#ifdef PHUB_PERF_DIAG
            if (my_node_.role == Node::SCHEDULER) return;
            auto str = PerfMonLite::Get().Summarize();
            //printf("[%d]PHUB Diagnostics:\n%s\n", dbgMyId, str.c_str());
#endif
        }

        static uint32_t getDataSignature(SArray<char> data)
        {
            uint32_t result = 0;
            SArray<uint32_t> arr = SArray<uint32_t>(data);
            for (size_t i = 0; i < arr.size(); i++)
            {
                result ^= arr[i];
            }
            return result;
        }

        virtual int RecvMsg(Message * msg) override
        {
            if (my_node_.role == ps::Node::Role::SCHEDULER)
            {
                return ZMQVan::RecvMsg(msg);
            }
            //return ZMQVan::RecvMsg(msg);
              //is there any control message? -- Ask ZMQ!
              //control messages have higher priorities.
            int recv_bytes = 0;
            msg->data.clear();
            //printf("RecvMsg called\n");
            //if my role is server, i need to simultaneously listen to all connections 
            //this should not return unless an item is received.
            int iter = 0;
            const int MASK = 16383;
            static uint64_t memoizedPoll = 0;
            //printf("[%d]recvBuffer is activated\n",ps::Postoffice::Get()->van()->my_node().id);
            while (true)
            {
                if (dataThroughInfiniband == false || (iter++ & MASK) == 0)
                {
                    recv_bytes = RecvCtrlMsgNoWait(msg);
                    //printf("recv_bytes : %d, dataThroughInfiniband : %d, memoIdx=%d\n", recv_bytes, dataThroughInfiniband, memoIdx);
                    if (recv_bytes > 0)
                    {
                        //printf("[ZMQ Delivery][%d][%d]wants to deliver this message up %s\n", dbgMyId,my_node_.role,msg->DebugString().c_str());
                        return recv_bytes;
                    }
                    else if (dataThroughInfiniband == false)
                    {
                        //sleep(0);
                        //std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        //printf("retrying...\n");
                        continue;
                        //dont run on infiniband when it's not ini ialized.
                    }
                }
                ibv_wc wc;
                bool any = false;
                //printf("[%d]Polling Completion Queue id = %d\n",msg->meta.recver,j);
        //poll once this and once that.
        //dumb to repeatedly poll from cq 0.
        //be smarter and poll in ascending.
                for (auto cqidx = 0; cqidx < verbs->core2CQIdxs.at(0).size(); cqidx++)
                {
                    auto cq = memoizedPoll++ % verbs->core2CQIdxs[0].size();
                    if (0 == verbs->poll(1, cq, Verbs::CompletionQueueType::Receive, &wc))
                    {
                        //  if(my_node_.id == 11)
                        //    printf("[%d] polling CQ = %d\n", my_node_.id, pollcnt++);
                        //sleep(0);
                        //Is there anything in the magical pull queue?
                        /*if (HasFeature(Van::PullRequestElision))
                        {
                            uint32_t qData;
                            if (pPHubWorkerSidePullQueue->TryDequeueSingleThreaded(&qData))
                            {
                                CHECK(false);
                                wc.imm_data = qData;
                                wc.wr_id = ReceiveRequests.size();
                                any = true;
                                break;
                            }
                }*/
                    }
                    else
                    {
                        any = true;
                        break;
                    }
                }
                if (any == false) continue;
                //.raise(SIGTRAP);
                        /*if(!(wc.opcode & IBV_WC_RECV))
                        {
                            if(wc.status != IBV_WC_SUCCESS)
                            {

                            LOG(INFO) << " received non receive completion event " << wc.wr_id
                                  << " status = " << wc.status
                                  << " src_qp " << wc.src_qp
                                  << " qp_num " << wc.qp_num
                                  << " wc.opcode = " << wc.opcode;
                            }
                            continue;
                        }*/

                        //post buffer now.
                if (wc.wr_id < ReceiveRequests.size())
                {
                    ReceiveRequests[wc.wr_id]->PostReceiveRequest();
                }
                else
                {
                    CHECK(wc.wr_id == ReceiveRequests.size());
                    CHECK(HasFeature(Van::PullRequestElision));
                }
                //printf("[%d] received.... \n", my_node_.id);
                auto rid = (wc.imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK;
                auto i = rid; //machine index.
                auto mit = node2MachineIdx[i];
                CHECK(mit != -1) << my_node_.id << ", " << my_node_.role << " asks who is " << i << "? (key=" << (wc.imm_data & PHUB_IMM_KEY_MASK) << "),(imm=" << wc.imm_data << ")";
                i = mit;
                auto j = wc.imm_data & PHUB_IMM_KEY_MASK;
                //printf("[%d][RECV][PREMIER]mid = %d, key = %d\n", my_node_.id, rid, j);

                CHECK(msg->meta.simple_app == false);
#ifdef PHUB_PERF_DIAG
                msg->meta.control.msg_sig = j;
#endif
                if (my_node_.role == Node::WORKER)
                {
                    auto& buffer = wRecvBuffer[i][j];
                    if (HasFeature(Van::MetadataElision) == false)
                    {
                        buffer.pMetaSlimBuffer->Populate(&msg->meta);
                        CHECK(msg->meta.request == false);
                        CHECK(buffer.KeyVal == j);

                        if (msg->meta.push)
                        {
                            //PUSH ACK
                            MEASURE_THIS_TIME_BEG_EX(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK, j, buffer.pMetaSlimBuffer->PHubAdditionalPayload);
                            MEASURE_THIS_TIME_END(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK, j);
                            CHECK(msg->meta.push);
                            //this path makes sure pullrequestelision and metadata elision are both off.
                            CHECK(wc.byte_len == keySize[j] * HasFeature(Van::PullRequestElision) + sizeof(MetaSlim)) << "[" << ps::Postoffice::Get()->van()->my_node().id << "] received len = " << wc.byte_len << " vs " << keySize[j] << " for key = " << j << msg->DebugString();

                            //i received a push request with 0 payload, remote is a server. this is a push ack.
                            //MEASURE_THIS_TIME_BEG(PERFMON_WORKER_PUSH_ACK_PIPELINE, j);
                        }
                        else
                        {
                            //CHECK(HasFeature(Van::PullRequestElision) == false) << "A van supports Pull request elision but is still sending pull requests.";
                            //PULL RESPONSE
                            MEASURE_THIS_TIME_BEG_EX(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK, j, buffer.pMetaSlimBuffer->PHubAdditionalPayload);
                            MEASURE_THIS_TIME_END(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK, j);
                            //the following check is disabled for RDMA
                                        //CHECK(wc.byte_len == wRecvBuffer[i][j].KVLength + sizeof(MetaSlim)) << wc.byte_len << " vs " << wRecvBuffer[i][j].KVLength + sizeof(MetaSlim) << " i,j = " << i << "," << j;
                                        //this one has payload
                                        //0 copy
                            msg->data.push_back(SArray<char>(buffer.KeySArray));
                            //printf("[Worker] received key = %d, Buffer = %s\n", j, SummarizeContinousBuffer((float*)buffer.KVBuffer, buffer.KVLength/sizeof(float)).c_str());
                            msg->data.push_back(SArray<char>(buffer.pKVBuffer, keySize[buffer.KeyVal]));
                            msg->data.push_back(SArray<char>(buffer.LenSArray));
                            //this is from server, responding to my pull request i am a worker.
                        }
                        /*if(my_node_.id == 9)*/
            //printf("[%d][WORKER][RECV] key = %d, from %d, byte_len=%d, ts=%d.\n", dbgMyId, j, rid, wc.byte_len, msg->meta.timestamp);
                    }
                    else
                    {
                        /*Metadata Elision*/
                        //make sure pull elision and metadata elision, otherwise indifferentiatable unless squeeze another bit in
                        //imm_data
                        CHECK(HasFeature(Van::PullRequestElision));
                        if (verbs->VerbsBitwidthReductionRatio == 1.0)
                            CHECK(wc.byte_len == keySize[j]) << "[" << ps::Postoffice::Get()->van()->my_node().id << "] received len = " << wc.byte_len << " vs " << keySize[j] << " for key = " << j << msg->DebugString();
                        msg->meta.body = "";
                        msg->meta.control.cmd = Control::EMPTY;
                        msg->meta.customer_id = 0;
                        msg->meta.head = 0;
                        msg->meta.push = true;
                        msg->meta.recver = my_node().id;
                        msg->meta.sender = rid;
                        msg->meta.simple_app = false;
                        msg->meta.timestamp = j;

                    }

                }//END OF WORKER PATH
                else
                {
                    auto& buffer = psRecvBuffer[i][j];
                    buffer.pMetaSlimBuffer->Populate(&msg->meta);
                    CHECK(msg->meta.request == true);
                    CHECK(buffer.KeyVal == j);

                    //SERVER PATH
                    if (msg->meta.push == false)
                    {
                        //PULL
                        MEASURE_THIS_TIME_BEG_EX(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL, j, buffer.pMetaSlimBuffer->PHubAdditionalPayload);
                        MEASURE_THIS_TIME_END(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL, j);
                        //remote is a worker. it sent me a pull request. i am a server
                        msg->data.push_back(SArray<char>(buffer.KeySArray));
                        msg->data.push_back(SArray<char>());
                    }
                    //nothing else needs to be done.
                    else
                    {
                        //PUSH
                        MEASURE_THIS_TIME_BEG_EX(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH, j, buffer.pMetaSlimBuffer->PHubAdditionalPayload);
                        MEASURE_THIS_TIME_END(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH, j);
                        //The following check is disabled for RDMA operations
                                    //CHECK(wc.byte_len == buffer.KVLength + sizeof(MetaSlim)) << wc.byte_len << " vs " << buffer.KVLength + sizeof(MetaSlim) << " i,j = " << i << "," << j;
                                    //this one has payload
                                    //0 copy
                        msg->data.push_back(SArray<char>(buffer.KeySArray));
                        //printf("[Worker] received key = %d, Buffer = %s\n", j, SummarizeContinousBuffer((float*)buffer.KVBuffer, buffer.KVLength/sizeof(float)).c_str());
                        msg->data.push_back(SArray<char>(buffer.pKVBuffer, keySize[buffer.KeyVal]));
                        msg->data.push_back(SArray<char>(buffer.LenSArray));
                        //printf("ts2Key[%d][%d]=%d\n",i, msg->meta.timestamp,j);
                        //ts2Key[i][msg->meta.timestamp] = j;
                    }
                    //printf("[%d][SERVER][RECV] from %d, byte_len=%d, key = %d, imm=%d, msg=%s.\n", dbgMyId, rid, wc.byte_len, j, wc.imm_data, msg->DebugString().c_str());

                }

                //if(my_node_.role == Node::SERVER)
                //auto signature = getDataSignature(msg->data[1]);
                //printf("[%d][%d]Posting RecvBuffer[%d][%d]. Msg from %d. [BUFFER SIGNATURE]=%d\nMSG:%s\n",dbgMyId, my_node().role,i,j,wc.imm_data,recvBuffer[i][j].DbgDataSignature(),msg->DebugString().c_str());
                //if(my_node_.role == Node::SERVER)
                //printf("[%d]Done posting RecvBuffer[%d][%d] Msg from %d\n",dbgMyId,i,j,wc.imm_data);
                msg->meta.recver = my_node_.id;
                msg->meta.sender = rid;

                return keySize[j] + sizeof(Meta);
            }
            //printf("[%d]RecvMsg has finished\n",ps::Postoffice::Get()->van()->my_node().id);
        }

        virtual std::string VanType() override
        {
            return "infiniband";
        }
        //SendMsg must be 100% threadsafe.
        virtual int SendMsg(const Message & msg) override
        {
            if (my_node_.role == ps::Node::Role::SCHEDULER)
            {
                return ZMQVan::SendMsg(msg);
            }
            //printf("SendMsg should be non-blocking\n");
            //PS_VLOG(2)<< my_node_.id << " sending "<< msg.DebugString();
            //print_stacktrace();
            //return ZMQVan::SendMsg(msg);
            if (msg.meta.control.empty() == false || dataThroughInfiniband == false || msg.meta.simple_app == true /*We don't know how to deal with simple apps*/)
            {
                //LOG(INFO)<<"data is transmitted through ZMQ "<<msg.DebugString();
                  //CHECK_EQ(msg.data.size(), 0);
                //printf("[zmq][%d][%d] ZMQ sending data %s\n",my_node_.id, my_node_.role, msg.DebugString().c_str());
                return ZMQVan::SendMsg(msg);
                //go to ZMQ please.
            }

            CHECK(msg.meta.body.empty()) << msg.meta.body;
            //now we have a meta that is fix sized.
            //disallow any meta data.
            //send only data.
            //infiniband is thread safe.
            CHECK(msg.meta.head == Control::EMPTY) << msg.meta.head;
            //node2MachineIdx lookup is threadsafe.
            auto id = node2MachineIdx[msg.meta.recver];
            CHECK(id != -1) << " No route to id = " << msg.meta.recver;// << id;
        /*if(my_node_.role == Node::WORKER && my_node_.id == 9)
      {*/
      //printf("[%d][%s][SEND] key = %d, wants to send %s to %d, id = %d.\n", dbgMyId, my_node_.role == Node::SERVER ? "SERVER" : "WORKER", msg.data.size() > 0 ? *((Key*)msg.data[0].data()) : -1, msg.DebugString().c_str(), msg.meta.recver, id);
/*}*/
//msg.data.size() > 0
            if (msg.data.size() == 0)
            {
                //this is a fast ack message
                //MetaSlim metaSlim(&msg.meta);
                //MEASURE_THIS_TIME_END(PERFMON_PHUB_PUSH_PIPELINE, msg.meta.control.msg_sig);
                CHECK(my_node_.role == Node::SERVER);
                CHECK(msg.meta.control.msg_sig < keySize.size()) << "[" << my_node_.id << "]" << msg.meta.control.msg_sig << " v.s. " << keySize.size();
                //only server shall receive this key.
                psSendBuffer[id][msg.meta.control.msg_sig].PostSendBufferFastAck(&msg.meta);
                //erase it.
                return sizeof(Meta);
            }
            else if (msg.data.size() == 2 && msg.data[1].size() == 0)
            {
                //this is a pull request. with keys only
                SArray<Key> keys = SArray<Key>(msg.data[0]);
                CHECK_EQ(keys.size(), 1);
                CHECK(msg.meta.data_type[0] == DataType::UINT64);
                CHECK(msg.meta.push == false);
                CHECK(msg.meta.request == true);
                if (HasFeature(Van::PullRequestElision) == false)
                {
                    wSendBuffer[id][keys[0]].PostSendBufferPull(&msg.meta);
                }
                else
                {
                    CHECK(false) << " Pull Elision Failure";
                    //For elided pull requests, we need to set the right flags.
            /* auto pMetaSlim = wRecvBuffer[id][keys[0]].pMetaSlimBuffer;
                    pMetaSlim->From(&msg.meta);
                    pMetaSlim->request = false;
                    //pMetaSlim->From(&msg
            //somehow signal recvmsg thread.
            //who does this key belong to?
                    auto qidx = verbs->workerKey2QPIdx[keys[0]];
                    auto sidx = verbs->workerQP2SrvId[qidx];
                    auto sid = 8 + sidx * 2; //maps back to server real ID.
                    auto imm = (sid << PHUB_MAX_KEY_BITS) | keys[0];
                    //printf("[%d] pull elided key = %d, srv = %d\n", my_node_.id, keys[0], sid);
                    pPHubWorkerSidePullQueue->Enqueue(imm);*/
                }
                return sizeof(Meta) + sizeof(Key);
            }
            else
            {
                SArray<Key> keys = SArray<Key>(msg.data[0]);
                //if(keys.size()!=1)
                //  {
                //    printf("CHK IMPL SENDMSG KSIZE = %d\n",keys.size());
                //    print_stacktrace();
                //   }
                CHECK(msg.data.size() >= 2);
                CHECK_EQ(keys.size(), 1);
                CHECK_EQ(msg.data[1].size(), keySize[keys[0]]) << msg.data[1].size() << " vs " << keySize[keys[0]];
                const_cast<Message&>(msg).meta.sender = ps::Postoffice::Get()->van()->my_node().id;
                //CHECK(msg.meta.sender != Meta::kEmpty);
                //now copy this to 
                //send everything in one shot.
                //server side still needs to copy the data to buffer if infiniband. note that PHUB does not require this.
                if (my_node_.role == Node::SERVER)
                {
                    //Infiniband server is not 0 copy capable, although it is possible to make it so.
                    //the worker side is 0 copy.
                    //Server PULL ACK
                    memcpy(psSendBuffer[id][keys[0]].pKVBuffer, msg.data[1].data(), msg.data[1].size());
                    psSendBuffer[id][keys[0]].PostSendBuffer(&msg.meta);
                }
                else
                {
                    //PUSH 
                    wSendBuffer[id][keys[0]].PostSendBuffer(&msg.meta);
                    //printf("[IBVERBS][%d][%d] wants to deliver meta=%s to %d. \n", dbgMyId, keys[0],wSendBuffer[id][keys[0]].pMetaSlimBuffer->DebugString().c_str(), msg.meta.recver);

                }
                //    
                //printf("[IBVERBS] key = %d, %p handoff = %s\n",keys[0],sendBuffer[id->second][keys[0]].KVBuffer, SummarizeContinousBuffer((float*)sendBuffer[id->second][keys[0]].KVBuffer,sendBuffer[id->second][keys[0]].KVLength / sizeof(float)).c_str());

                /* if(keys[0]==0) { */
                /*   printf("[IBVERBS] key = %d, %p handoff = %s\n",keys[0],wSendBuffer[id][keys[0]].pKVBuffer, SummarizeContinousBuffer((float*)wSendBuffer[id][keys[0]].pKVBuffer,wSendBuffer[id][keys[0]].KVLength / sizeof(float)).c_str()); */
                /* } */

               //uint64_t signature = getDataSignature(msg.data[1]);

                return keySize[keys[0]] + sizeof(Meta) - sizeof(MetaSlim);
            }
            CHECK(false);
        }

        virtual bool FullyInitialized() override
        {
            return dataThroughInfiniband;
        }
        virtual int GetUnderlyingWorkerThreadCount()
        {
            return 1;
        }
    };
}
