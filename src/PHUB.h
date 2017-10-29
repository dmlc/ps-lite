#pragma once
#include "dmlc/logging.h"
#include "dmlc/DIME.h"
#include <ps/internal/van.h>
#include <ps/internal/postoffice.h>
#include <ps/internal/message.h>
#include <sstream>
#include <malloc.h> //memalign
#include "Optimizers.h"
#include <signal.h>
#include <limits.h>
#include "Verbs.hpp"
/*#define PHUB_MAX_KEY_BITS 22
#define PHUB_MAX_KEY ((1<<PHUB_MAX_KEY_BITS) - 1)
#define PHUB_IMM_KEY_MASK PHUB_MAX_KEY
#define PHUB_IMM_SENDER_MASK ((1<<(32 - PHUB_MAX_KEY_BITS))-1)*/

namespace ps
{

    struct MetaSlim
    {
        MetaSlim()
        {
          CHECK(sizeof(*this)==64);
        }
        int head;
        /** \brief the unique id of the customer is messsage is for*/
        int customer_id;
        /** \brief the timestamp of this message */
        int timestamp;
        /** \brief the node id of the sender of this message */
        int sender;
        /** \brief the node id of the receiver of this message */
        int recver;
        /** \brief whether or not this is a request message*/
        bool request;
        /** \brief whether or not a push message */
        bool push;
        /** \brief whether or not it's for SimpleApp */
        bool simple_app;
#ifdef PHUB_PERF_DIAG
        /* PHUB multiplexed, usually a timestamp for perf purposes*/
        uint64_t PHubAdditionalPayload;
#else
        uint64_t EightBytesToPadTo32Bytes;
#endif

        char dummy[32];

        MetaSlim(const Meta* from)
        {
            From(from);
        }

        void Populate(Meta* meta)
        {
            meta->head = head;
            meta->control.cmd = Control::Command::EMPTY;
            meta->customer_id = customer_id;
            //key TODO:type ignored.
            //meta->data_type.push_back(DataType::UINT64);
            //meta->data_type.push_back(DataType::FLOAT);
            meta->body = "";
            meta->push = push;
            meta->request = request;
            meta->recver = recver;
            meta->sender = sender;
            meta->simple_app = simple_app;
            meta->timestamp = timestamp;
        }

        void From(const Meta* from)
        {
            head = from->head;
            customer_id = from->customer_id;
            timestamp = from->timestamp;
            sender = from->sender;
            recver = from->recver;
            request = from->request;
            push = from->push;
            simple_app = from->simple_app;
        }

        std::string DebugString()
        {
            std::stringstream ss;
            ss << "MetaSlim: push=" << push << " request=" << request << " recver=" << recver << " sender=" << sender << " simple_app=" << simple_app << " timestamp=" << timestamp << "\n";
            return ss.str();
        }
    };




    //Specialized IBBuffer for worker
    struct WorkerIBBuffer
    {
        char* pKVBuffer;
        ibv_mr* MemoryRegionInformation;
        ibv_recv_wr ReceiveRequest;
        //ibv_send_wr ReceiveRequestFastAckSendRequest;
        ibv_send_wr SendRequest;
        //ibv_sge ScatterGatherElement;
        //ibv_sge ScatterGatherElementForMetaSlim;
        ibv_sge* MetaSge;
        ibv_sge* KVSge;
        //ibv_sge ScatterGatherElementFastAck;
        int KVLength;
        size_t ActualKVSizePaddedForSSE;
        int FilledLength;
        Key KeyVal;
        int QPIndex;
        Verbs* AssociatedVerbs;
        SArray<Key> KeySArray;
        SArray<int> LenSArray;
        bool IsSendBuffer;
        MetaSlim* pMetaSlimBuffer;
        bool initialized = false;
        int CQIndex;
        ibv_sge SgeArray[2];
        char* RemoteTotalBuffer;
        int MachineIndex;
        void Init(Verbs* verbs,
            int machineIndex,
            int kvlen,
            Key key,
            bool isSendBuffer,
            char* preallocatedKVBuffer,
            MetaSlim* preallocatedMetaBuffer,
            char* remoteTotalBuffer,
            ibv_mr* memReg)
        {
            CHECK(machineIndex >= 0);
            CHECK(remoteTotalBuffer != NULL);
            CHECK(verbs != NULL);
            CHECK(memReg != NULL);
            CHECK(preallocatedMetaBuffer != NULL);
            CHECK(preallocatedKVBuffer != NULL || kvlen == 0);


            MachineIndex = machineIndex;
            RemoteTotalBuffer = remoteTotalBuffer;
            MetaSge = &SgeArray[0];
            KVSge = &SgeArray[1];
            IsSendBuffer = isSendBuffer;
            AssociatedVerbs = verbs;
            KVLength = kvlen;
            ActualKVSizePaddedForSSE = RoundUp(kvlen / sizeof(float), INSTRUCTION_VECTOR_SIZE) * sizeof(float);

            MemoryRegionInformation = memReg;
            pKVBuffer = preallocatedKVBuffer;
            pMetaSlimBuffer = preallocatedMetaBuffer;
            CHECK(pMetaSlimBuffer != NULL);
            //CHECK((uint64_t)preallocatedMetaBuffer == (uint64_t)preallocatedKVBuffer - sizeof(MetaSlim));
            //RDMA NO NEED Recev
            //memset(&ReceiveRequest, 0, sizeof(ibv_recv_wr));
            memset(SgeArray, 0, sizeof(ibv_sge) * 2);
            memset(&SendRequest, 0, sizeof(ibv_send_wr));

            MetaSge->addr = (uint64_t)preallocatedMetaBuffer;
            MetaSge->length = sizeof(MetaSlim);
            MetaSge->lkey = MemoryRegionInformation->lkey;

            KVSge->addr = (uint64_t)preallocatedKVBuffer;
            KVSge->length = kvlen;

            if (kvlen / verbs->VerbsBitwidthReductionRatio > sizeof(float))
            {
                KVSge->length /= verbs->VerbsBitwidthReductionRatio;
            }


            KVSge->lkey = MemoryRegionInformation->lkey;

            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false)
            {
                SendRequest.num_sge = 2;
                //SendRequest.send_flags = IBV_SEND_SIGNALED;
                SendRequest.sg_list = SgeArray;
            }
            else
            {
                //no metadata payload
                SendRequest.num_sge = 1;
                SendRequest.sg_list = KVSge;
            }

            SendRequest.next = NULL;
            SendRequest.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            SendRequest.imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false)
            {
                SendRequest.wr.rdma.remote_addr = (uint64_t)remoteTotalBuffer;
            }
            else
            {
                SendRequest.wr.rdma.remote_addr = (uint64_t)remoteTotalBuffer + sizeof(MetaSlim);
            }
            SendRequest.wr.rdma.rkey = verbs->Helper_Worker_GetEndpointFromKey(key).RemoteKey;

            CHECK(SendRequest.wr.rdma.rkey != 0);

            //RDMA uses completion with 0 byte.
            ReceiveRequest.num_sge = 0;
            ReceiveRequest.sg_list = NULL;
            ReceiveRequest.next = NULL;

            QPIndex = verbs->workerKey2QPIdx[key];
            CHECK(key < PHUB_MAX_KEY);
            KeyVal = key;
            KeySArray.push_back(KeyVal);
            LenSArray.push_back(KVLength / sizeof(float));
            CQIndex = AssociatedVerbs->Helper_Worker_GetEndpointFromKey(KeyVal).CQIdx;
            initialized = true;
        }

        void PostSendBufferPull(const Meta* meta)
        {
            CHECK(ps::Postoffice::Get()->van()->HasFeature(ps::Van::PullRequestElision) == false);
            CHECK(ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false);
            /*printf("[%d][%d]Posting QuickAck/Pull QPIndex=%d, [%d]->[%d], [%d]=>[%d]\n", ps::Postoffice::Get()->van()->my_node().id,ps::Postoffice::Get()->van()->my_node().role,
            QPIndex,
            AssociatedVerbs->port_attributes.lid,
            AssociatedVerbs->endpoints[QPIndex].lid,
            AssociatedVerbs->endpoints[QPIndex].queue_pair->qp_num,
            AssociatedVerbs->endpoints[QPIndex].qp_num);*/

            CHECK(initialized);
            SendRequest.num_sge = 1;
            //metadata only.
            pMetaSlimBuffer->From(meta);
            //need to change size!
            //printf("[%d][WORKER] pushing out pull with wc_imm_data=%d, keyval=%d, metaSlim=%s, QPIdx=%d, CQIdx=%d",ps::Postoffice::Get()->van()->my_node().id, SendRequest.imm_data,KeyVal, pMetaSlimBuffer->DebugString().c_str(),QPIndex, CQIndex );
            TIMESTAMP_METASLIM_PTR(pMetaSlimBuffer);

            AssociatedVerbs->VerbsSmartPost(QPIndex, CQIndex, 1, &SendRequest);

            //nothing to poll
        }

        void PostSendBuffer(const Meta* meta)
        {
            //send myself.
            CHECK(initialized) << " msg : " << meta->DebugString() << " key = " << KeyVal << " remote = " << MachineIndex;
            /*printf("[%d][WORKER]Posting Send QPIndex=%d, [%d]->[%d], [%d]=>[%d]\n", ps::Postoffice::Get()->van()->my_node().id,
            QPIndex,
            AssociatedVerbs->port_attributes.lid,
            AssociatedVerbs->endpoints[QPIndex].lid,
            AssociatedVerbs->endpoints[QPIndex].queue_pair->qp_num,
            AssociatedVerbs->endpoints[QPIndex].qp_num);
        */
        //CHECK(sizeof(MetaSlim) + data.size() <= Length);
        //meta data and data.
            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false)
            {
                SendRequest.num_sge = 2;
                pMetaSlimBuffer->From(meta);
                TIMESTAMP_METASLIM_PTR(pMetaSlimBuffer);
            }
            AssociatedVerbs->VerbsSmartPost(QPIndex, CQIndex, 1, &SendRequest);
            //printf("[%d][%d] pushing out sendreq with wc_imm_data=%d, keyval=%d, metaSlim=%s, QPIndex=%d, CQIdx=%d\n",ps::Postoffice::Get()->van()->my_node().id, ps::Postoffice::Get()->van()->my_node().role,SendRequest.imm_data,KeyVal, MetaSlimBuffer.DebugString().c_str(), QPIndex, CQIndexFromKey);
        }
    };
    //PSHUB requires only 1 sge per message, instead of 2.
    //PSHUB requires sending out 2 messages.
    struct PSIBBuffer
    {
        char* pKVBuffer;
        ibv_mr* MemoryRegionInformation;
        ibv_recv_wr ReceiveRequest;
        //ibv_send_wr ReceiveRequestFastAckSendRequest;
        ibv_send_wr SendMetaRequest;
        ibv_send_wr SendKVRequest;
        //ibv_sge ScatterGatherElement;
        //ibv_sge ScatterGatherElementForMetaSlim;
        ibv_sge MetaSge;
        ibv_sge KVSge;
        //ibv_sge ScatterGatherElementFastAck;
        int KVLength;
        size_t ActualKVSizePaddedForSSE;
        int FilledLength;
        Key KeyVal;
        int QPIndex;
        Verbs* AssociatedVerbs;
        SArray<Key> KeySArray;
        SArray<int> LenSArray;
        bool IsSendBuffer;
        MetaSlim* pMetaSlimBuffer;
        bool initialized = false;
        int CQIndex;
        char* RemoteKVBuffer;
        MetaSlim* RemoteMetaSlimBuffer;
        int MachineIndex;
        bool IsInitialized()
        {
            return initialized;
        }

        uint32_t DbgDataSignature()
        {
            uint32_t result = 0;
            auto start = pKVBuffer;
            auto ptr = (uint32_t*)start;
            int sz = (KVLength) / sizeof(uint32_t);
            /*printf("[DBGDTSIG]Data Len = %d, Total Len = %d. Data as INT32=\n",sz, Length);*/
            for (size_t i = 0; i < sz; i++)
            {
                result ^= ptr[i];
            }
            return result;
        }
        void Init(Verbs* verbs,
            int machineIndex,
            int kvlen,
            Key key,
            bool isSendBuffer,
            char* preallocatedKVBuffer,
            MetaSlim* preallocatedMetaBuffer,
            char* remoteKVBuffer,
            MetaSlim* remoteMetaBuffer,
            ibv_mr* memReg)
        {
            CHECK(remoteKVBuffer != NULL);
            CHECK(remoteMetaBuffer != NULL);
            CHECK(memReg != NULL);
            CHECK(preallocatedMetaBuffer != NULL);
            CHECK(preallocatedKVBuffer != NULL || kvlen == 0);
            CHECK(machineIndex >= 0);
            CHECK(verbs != NULL);

            MachineIndex = machineIndex;
            RemoteKVBuffer = remoteKVBuffer;
            RemoteMetaSlimBuffer = remoteMetaBuffer;
            IsSendBuffer = isSendBuffer;
            AssociatedVerbs = verbs;
            KVLength = kvlen;
            ActualKVSizePaddedForSSE = RoundUp(kvlen / sizeof(float), INSTRUCTION_VECTOR_SIZE) * sizeof(float);

            MemoryRegionInformation = memReg;
            pKVBuffer = preallocatedKVBuffer;
            pMetaSlimBuffer = preallocatedMetaBuffer;
            CHECK((uint64_t)preallocatedMetaBuffer == (uint64_t)preallocatedKVBuffer - sizeof(MetaSlim));
            memset(&ReceiveRequest, 0, sizeof(ibv_recv_wr));
            memset(&MetaSge, 0, sizeof(ibv_sge));
            memset(&KVSge, 0, sizeof(ibv_sge));
            memset(&SendMetaRequest, 0, sizeof(ibv_send_wr));
            memset(&SendKVRequest, 0, sizeof(ibv_send_wr));

            MetaSge.addr = (uint64_t)preallocatedMetaBuffer;
            MetaSge.length = sizeof(MetaSlim);
            MetaSge.lkey = MemoryRegionInformation->lkey;

            KVSge.addr = (uint64_t)preallocatedKVBuffer;
            KVSge.length = kvlen;

            if (kvlen / verbs->VerbsBitwidthReductionRatio > sizeof(float))
            {
                KVSge.length /= verbs->VerbsBitwidthReductionRatio;
            }

            KVSge.lkey = MemoryRegionInformation->lkey;

            CHECK(ps::Postoffice::Get()->van()->my_node().id != ps::Meta::kEmpty);

            SendMetaRequest.num_sge = 1;
            SendMetaRequest.next = &SendKVRequest;
            SendMetaRequest.sg_list = &MetaSge;
            SendMetaRequest.opcode = IBV_WR_RDMA_WRITE;
            SendMetaRequest.imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
            SendMetaRequest.wr.rdma.remote_addr = (uint64_t)remoteMetaBuffer;
            SendMetaRequest.wr.rdma.rkey = verbs->Helper_Server_GetEndpointFromKey(key, machineIndex).RemoteKey;

            SendKVRequest.num_sge = 1;
            SendKVRequest.next = NULL;
            //SendKVRequest.send_flags = IBV_SEND_SIGNALED;
            SendKVRequest.imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
            SendKVRequest.sg_list = &KVSge;
            SendKVRequest.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            SendKVRequest.wr.rdma.remote_addr = (uint64_t)remoteKVBuffer;
            SendKVRequest.wr.rdma.rkey = verbs->Helper_Server_GetEndpointFromKey(key, machineIndex).RemoteKey;

            //only wants to imm.
            ReceiveRequest.num_sge = 0;
            ReceiveRequest.sg_list = NULL;
            ReceiveRequest.next = NULL;

            auto qpIdx = verbs->workerKey2QPIdx.at(key);
            auto srvIdx = verbs->workerQP2SrvId.at(qpIdx);
            CHECK(srvIdx == ps::Postoffice::Get()->my_rank());
            auto offset = verbs->workerQP2SrvQPIdx.at(qpIdx);
            QPIndex = MachineIndex * verbs->psQPCount + offset;
            CHECK(QPIndex >= 0 && QPIndex < verbs->endpoints.size());
            CHECK(key < PHUB_MAX_KEY);
            KeyVal = key;
            KeySArray.push_back(KeyVal);
            LenSArray.push_back(KVLength / sizeof(float));

            //with or without the machine index, it is the same.
            CQIndex = verbs->Helper_Server_GetEndpointFromKey(key, machineIndex).CQIdx;
            initialized = true;
        }
        ///Infiniband Servers
        void PostSendBufferFastAck(const Meta* meta)
        {
            /*printf("[%d][%d]Posting QuickAck/Pull QPIndex=%d, [%d]->[%d], [%d]=>[%d]\n", ps::Postoffice::Get()->van()->my_node().id,ps::Postoffice::Get()->van()->my_node().role,
            QPIndex,
            AssociatedVerbs->port_attributes.lid,
            AssociatedVerbs->endpoints[QPIndex].lid,
            AssociatedVerbs->endpoints[QPIndex].queue_pair->qp_num,
            AssociatedVerbs->endpoints[QPIndex].qp_num);*/


            //This is for INFINIBAND SERVER.
            CHECK(initialized);
            //metadata only.
            pMetaSlimBuffer->From(meta);
            SendMetaRequest.next = NULL;//Do not send payload - is not ready yet.
            //this is mandatory for waking up remote.
            SendMetaRequest.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            //need to change size!
        //printf("[%d][SERVER] pushing out fastack with mid = %d  keyval=%d, metaSlim=%s, QPIdx=%d, CQIdx=%d", ps::Postoffice::Get()->van()->my_node().id, MachineIndex, KeyVal, pMetaSlimBuffer->DebugString().c_str(), QPIndex, CQIndex);
            TIMESTAMP_METASLIM_PTR(pMetaSlimBuffer);
            AssociatedVerbs->VerbsSmartPost(QPIndex, CQIndex, 1, &SendMetaRequest);
        }
        ///Infiniband Servers
        void PostSendBuffer(const Meta* meta)
        {
            //For Infiniband servers.
            //send myself.
            CHECK(initialized);
            //if(sizeof(MetaSlim) + data.size() > Length)
            //  {
            //    printf("CHK IMPL sizeof(MetaSlim) = %d, data.size() = %d and Length is only %d", sizeof(MetaSlim), data.size(), Length);
            //  }
            //printf("[%d][%d]Posting Send QPIndex=%d, [%d]->[%d], [%d]=>[%d]\n", ps::Postoffice::Get()->van()->my_node().id,ps::Postoffice::Get()->van()->my_node().role,
            //QPIndex,
            //AssociatedVerbs->port_attributes.lid,
            //AssociatedVerbs->endpoints[QPIndex].lid,
            //AssociatedVerbs->endpoints[QPIndex].queue_pair->qp_num,
            //AssociatedVerbs->endpoints[QPIndex].qp_num);

            //CHECK(sizeof(MetaSlim) + data.size() <= Length);
        //this is a full payload
            TIMESTAMP_METASLIM_PTR(pMetaSlimBuffer);
            SendMetaRequest.next = &SendKVRequest;
            //Do not notify remote if it is to follow kvbuffers.
            SendMetaRequest.opcode = IBV_WR_RDMA_WRITE;
            pMetaSlimBuffer->From(meta);
            AssociatedVerbs->VerbsSmartPost(QPIndex, CQIndex, 2, &SendMetaRequest);
            //printf("[%d][SERVER] pushing out sendreq with mid=%d, keyval=%d, metaSlim=%s, QPIndex=%d, CQIdx=%d\n",ps::Postoffice::Get()->van()->my_node().id, MachineIndex, KeyVal, pMetaSlimBuffer->DebugString().c_str(), QPIndex, CQIndex);
        }
        std::string Summarize()
        {
            std::stringstream ss;
            ss << "MetaSge.addr = " << MetaSge.addr << " len = " << MetaSge.length << " lkey = " << MetaSge.lkey << "; KVSge.addr = " << KVSge.addr << " len = " << KVSge.length << " lkey = " << KVSge.lkey;
            printf("[%d][%d]%s, QPNum = %d,KeyVal = %d,encodedImm = %d, mid = %d, RemoteQPNUm = %d, QPIndex = %d, SendCQ = %p, RecvCQ = %p CQIndex = %d, KVBuffer=%p, KVLen=%d %s!\n",
                ps::Postoffice::Get()->van()->my_node().id,
                ps::Postoffice::Get()->van()->my_node().role,
                IsSendBuffer ? "SEND BUFFER" : "RECV BUFFER",
                AssociatedVerbs->endpoints[QPIndex].queue_pair->qp_num,
                KeyVal,
                SendMetaRequest.imm_data,
                ps::Postoffice::Get()->van()->my_node().id,
                AssociatedVerbs->endpoints[QPIndex].qp_num,
                QPIndex,
                NULL,//AssociatedVerbs->sendCompletionQueues[CQIndexFromKey],
                NULL,//AssociatedVerbs->receiveCompletionQueues[CQIndexFromKey],
                -1,//CQIndexFromKey,
                pKVBuffer,
                KVLength,
                ss.str().c_str()
            );
            return ss.str();
        }
    };
    //////////////////////////////////////////////////////////////////////////
    //For PHUB use only.
    struct PHubMergeBuffer
    {
    public:
        //volatile RemainingReadersForCurrentBuffer; //how many people in the current iteration have pulled?
        volatile int WriteBufferIndex __attribute__((aligned(CACHELINE_SIZE_BYTES))); //either 0 or 1. Buffer used for current iteration.
        //the actual buffer size that's used to align data for sse.
        size_t ActualBufferSizePaddedForSSE;
        size_t ActualElementCountPaddedForSSE;
        //ibv_send_wr ReceiveRequestFastAckSendRequest;
        int Length;
        int FilledLength;
        Key KeyVal;
        Verbs* AssociatedVerbs;
        bool initialized;
        int CQIndex = -1;
        bool PSInitialized;//the first push by rank 0 worker
        int QPOffset;

        PHubMergeBuffer()
        {
            initialized = false;
            PSInitialized = false;
        }

        bool Initialized()
        {
            return initialized;
        }
        std::vector<uint64_t> RDMAWorkerRecvBufferMetaAddrs;
        std::vector<ibv_send_wr> RDMAWorkerSendKVRequests;
        std::vector<ibv_send_wr> RDMAWorkerSendMetaRequests;
        //std::vector<ibv_sge> RDMAWorkerSendKVSges;
        //std::vector<ibv_sge> RDMAWorkerSendMetaSges;
        std::vector<int> remoteQPIdxs;

        std::vector<std::vector<ibv_sge>> RDMAWorkerSendSgesArray;
        void Init(Verbs* verbs, int bufferLen, Key key, char* pBuf1, char* pBuf2,
            std::vector<uint64_t> /*Copy it*/ rdmaWorkerRecvBufferMetaAddrs,
            std::vector<uint64_t> rdmaRemoteKVBuffers,
            std::vector<uint64_t> rdmaRemoteMetaBuffers)
        {
            CHECK(pBuf1 != NULL);
            CHECK(pBuf2 != NULL);
            CHECK(verbs != NULL);
            CHECK(sizeof(MetaSlim) % (INSTRUCTION_VECTOR_SIZE * sizeof(float)) == 0) << "MetaSlim's size made it not possible to align subsequent floats in MergeBuffer";
            Length = bufferLen;
            AssociatedVerbs = verbs;
            //RemainingReadersForCurrentBuffer = ps::Postoffice::Get()->num_workers();
            WriteBufferIndex = 0;
            //auto kvLen = bufferLen - sizeof(MetaSlim); // this is a terrible impl;
            ActualElementCountPaddedForSSE = RoundUp(bufferLen / sizeof(float), INSTRUCTION_VECTOR_SIZE);
            ActualBufferSizePaddedForSSE = ActualElementCountPaddedForSSE * sizeof(float);
            Buffer1 = pBuf1;
            memset(Buffer1, 0, ActualBufferSizePaddedForSSE);
            Buffer2 = pBuf2;
            memset(Buffer2, 0, ActualBufferSizePaddedForSSE);

            //The first sizeof(MetaSlim) bytes in buf1 and buf2 are not used in RDMA
            //copy it.
            int numWorkers = rdmaWorkerRecvBufferMetaAddrs.size();
            CHECK(ps::Postoffice::Get()->num_workers() == numWorkers);
            RDMAWorkerRecvBufferMetaAddrs = rdmaWorkerRecvBufferMetaAddrs;
            ibv_send_wr cleanWr;
            ibv_sge cleanSge;
            memset(&cleanSge, 0, sizeof(ibv_sge));
            memset(&cleanWr, 0, sizeof(ibv_send_wr));
            RDMAWorkerSendKVRequests.resize(numWorkers, cleanWr);
            RDMAWorkerSendMetaRequests.resize(numWorkers, cleanWr);
            //RDMAWorkerSendKVSges.resize(numWorkers, cleanSge);
            //RDMAWorkerSendMetaSges.resize(numWorkers, cleanSge);
            remoteQPIdxs.resize(numWorkers, -1);
            RDMAWorkerSendSgesArray.resize(numWorkers);

            KeyVal = key;

            auto qpIdx = verbs->workerKey2QPIdx.at(KeyVal);
            auto srvIdx = verbs->workerQP2SrvId.at(qpIdx);
            CHECK(srvIdx == ps::Postoffice::Get()->my_rank());
            QPOffset = verbs->workerQP2SrvQPIdx.at(qpIdx);
            for (size_t i = 0; i < numWorkers; i++)
            {
                RDMAWorkerSendSgesArray[i].resize(2, cleanSge);

                //SendKVRequests
                RDMAWorkerSendKVRequests.at(i).num_sge = 1;
                RDMAWorkerSendKVRequests.at(i).sg_list = &RDMAWorkerSendSgesArray[i][1];
                RDMAWorkerSendKVRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                RDMAWorkerSendKVRequests.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
                RDMAWorkerSendKVRequests.at(i).wr.rdma.remote_addr = rdmaRemoteKVBuffers[i];
                RDMAWorkerSendKVRequests.at(i).wr.rdma.rkey = verbs->Helper_Server_GetEndpointFromKey(key, i).RemoteKey;


                //SendMetaRequests
                RDMAWorkerSendMetaRequests.at(i).num_sge = 1;
                RDMAWorkerSendMetaRequests.at(i).sg_list = &RDMAWorkerSendSgesArray[i][0];
                RDMAWorkerSendMetaRequests.at(i).opcode = IBV_WR_RDMA_WRITE;
                RDMAWorkerSendMetaRequests.at(i).next = &RDMAWorkerSendKVRequests[i];
                RDMAWorkerSendMetaRequests.at(i).wr.rdma.remote_addr = (uint64_t)rdmaRemoteMetaBuffers[i];
                RDMAWorkerSendMetaRequests.at(i).wr.rdma.rkey = verbs->Helper_Server_GetEndpointFromKey(key, i).RemoteKey;


                //SendKVSges.
                RDMAWorkerSendSgesArray[i][1].length = Length - sizeof(MetaSlim);
                //enable bit reduction?
                if (RDMAWorkerSendSgesArray[i][1].length / verbs->VerbsBitwidthReductionRatio > sizeof(float))
                {
                    RDMAWorkerSendSgesArray[i][1].length /= verbs->VerbsBitwidthReductionRatio;
                }
                CHECK(RDMAWorkerSendSgesArray[i][1].addr == NULL); //<--variable.

                //SendMetaSges.
                RDMAWorkerSendSgesArray[i][0].length = sizeof(MetaSlim);
                RDMAWorkerSendSgesArray[i][0].addr = rdmaWorkerRecvBufferMetaAddrs[i];


                remoteQPIdxs.at(i) = QPOffset + verbs->psQPCount * i;

                if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision))
                {
                    //with metadata elision, we use the RDMAWorkerSendMetaRequests to actually transport payload
                    //terrible but allows us to change less code.
                    RDMAWorkerSendMetaRequests.at(i).num_sge = 1;
                    RDMAWorkerSendMetaRequests.at(i).sg_list = &RDMAWorkerSendSgesArray.at(i)[1];
                    RDMAWorkerSendMetaRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                    //index 1 is KV.
                    RDMAWorkerSendMetaRequests.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
                    RDMAWorkerSendMetaRequests.at(i).next = NULL;
                    //override the remote address.
                    RDMAWorkerSendMetaRequests.at(i).wr.rdma.remote_addr = (uint64_t)rdmaRemoteKVBuffers[i];

                }
                //we can save 1 message if the meta and kv are continuous on the worker side. test it here.
                else if (rdmaRemoteMetaBuffers.at(i) + sizeof(MetaSlim) == rdmaRemoteKVBuffers.at(i))
                {
                    CHECK(false) << " Elision off?";
                    //do not use two requests.
                    //make sure the correct opcode is used.
                    RDMAWorkerSendMetaRequests.at(i).num_sge = 2; //one for meta and one for kv.
                    RDMAWorkerSendMetaRequests.at(i).sg_list = RDMAWorkerSendSgesArray.at(i).data();
                    RDMAWorkerSendMetaRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
                    RDMAWorkerSendMetaRequests.at(i).imm_data = (ps::Postoffice::Get()->van()->my_node().id << PHUB_MAX_KEY_BITS) | key;
                    RDMAWorkerSendMetaRequests.at(i).next = NULL;
                }
                else
                {
                    CHECK(false) << " Elision off?";
                }
            }

            CHECK(((uint64_t)Buffer1 & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0) << " requested alignment is " << INSTRUCTION_VECTOR_SIZE * sizeof(float);
            CHECK(((uint64_t)Buffer2 & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
            //printf("mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, key);
            ActualElementCountPaddedForSSE = (ActualBufferSizePaddedForSSE - sizeof(MetaSlim)) / sizeof(float);
            CHECK(KeyVal < PHUB_MAX_KEY);
            initialized = true;

        }

        inline int GetKVBufferLen()
        {
            return Length - sizeof(MetaSlim);
        }

        inline int GetKVBufferLenPaddedForSSE()
        {
            return ActualBufferSizePaddedForSSE - sizeof(MetaSlim);
        }

        inline int GetKVElementCountAccountingForSSEPadding()
        {
            return ActualElementCountPaddedForSSE;
        }

        inline char* GetCurrentWriteBuffer()
        {
            //write buffer is always opposite from pull buffer.
            auto ret = WriteBufferIndex == 0 ? Buffer1 : Buffer2;
            //printf("[W]mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, KeyVal);

            return ret + sizeof(MetaSlim);
        }

        inline char* GetCurrentReadBuffer()
        {
            CHECK(PSInitialized);
            auto ret = WriteBufferIndex == 0 ? Buffer2 : Buffer1;
            //printf("[r]mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, KeyVal);

            return ret + sizeof(MetaSlim);
        }

        void AggregationAndOptimizationReady()
        {
            WriteBufferIndex = (WriteBufferIndex + 1) & 1;
        }

        ///Cookies injected.
        ///For non-accurate optimizations, make sure to check the first Sizeof(metaSlim) buffer is not touched.
        void PostSendBufferBundledPushPullAck(int remoteMachine)
        {
            auto buffer = GetCurrentReadBuffer();
            //o is meta, 1 is kv. 
            RDMAWorkerSendSgesArray[remoteMachine][1].addr = (uint64_t)buffer;
            auto& ep = AssociatedVerbs->Helper_Server_GetEndpointFromKey(KeyVal, remoteMachine);
            CQIndex = ep.CQIdx;
            auto lkey = AssociatedVerbs->DeviceMemoryRegions.at(ep.DeviceIdx)->lkey;
            //use the correct lkeys.
            //RDMAWorkerSendMetaSges.at(remoteMachine).lkey = RDMAWorkerSendKVSges[remoteMachine].lkey = lkey;
            //just in case.
            RDMAWorkerSendSgesArray.at(remoteMachine).at(0).lkey = RDMAWorkerSendSgesArray.at(remoteMachine).at(1).lkey = lkey;
            CHECK(remoteQPIdxs.at(remoteMachine) == ep.Index);
            //auto pMs = (MetaSlim*)RDMAWorkerRecvBufferMetaAddrs.at(remoteMachine);
        //printf("[PSHUB] Sending Bundled message to QPidx = %d, rid = %d \n", remoteQPIdxs.at(remoteMachine), 9 + remoteMachine * 2);
            int msgCnt = RDMAWorkerSendMetaRequests[remoteMachine].next == NULL ? 1 : 2;
	    //metadata elision
	    CHECK(msgCnt == 1);
            //printf("bundle message count = %d\n", msgCnt);
            AssociatedVerbs->VerbsSmartPost(remoteQPIdxs.at(remoteMachine), CQIndex, msgCnt, &(RDMAWorkerSendMetaRequests.at(remoteMachine)));
        }
        char* Buffer1;
        char* Buffer2;
    };

    struct IBReceiveRequest
    {
        size_t QPIndex __attribute__((aligned(CACHELINE_SIZE_BYTES)));
        Verbs* AssociatedVerbs;
        ibv_recv_wr ReceiveRequest;
        IBReceiveRequest(size_t qpIndex, Verbs* verbs, size_t id)
        {
            QPIndex = qpIndex;
            AssociatedVerbs = verbs;
            memset(&ReceiveRequest, 0, sizeof(ibv_recv_wr));
            ReceiveRequest.wr_id = id;
        }

        void PostReceiveRequest()
        {
            //printf("[%d]Post Recv Request QPI=%d, ID=%d\n", ps::Postoffice::Get()->van()->my_node().id,  QPIndex,  ReceiveRequest.wr_id);
            AssociatedVerbs->post_receive(QPIndex, &ReceiveRequest);
        }
    };
}
