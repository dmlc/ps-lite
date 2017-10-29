#pragma once

#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <ps/internal/postoffice.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream> 
#include <pthread.h>
#include <cstring>
#include <dmlc/DIME.h>
#include <dmlc/logging.h>
#include "PHubAllocator.h"
#include "consts.h"
#include <numa.h>
#include "Helpers.h"
/*#define PHUB_MAX_KEY_BITS 22
#define PHUB_MAX_KEY ((1<<PHUB_MAX_KEY_BITS) - 1)
#define PHUB_IMM_KEY_MASK PHUB_MAX_KEY
#define PHUB_IMM_SENDER_MASK ((1<<(32 - PHUB_MAX_KEY_BITS))-1)
*/
//class PHubAllocator; //...
using namespace ps;
using namespace std;

///
/// This class does the bare minimum to up IBVerbs queue pairs between
/// all processes in an MPI job for RDMA communications. It uses MPI
/// to exchange information during queue pair setup.
///
/// It assumes you won't be using Send/Receive Verbs and doesn't post
/// any receive buffers, although you certainly can if you want.
///
/// It assumes you will be running multiple processes per node, and
/// gives each process two IDs ("ranks") and synchronization domains:
///
///  - one that is valid across all processes on all nodes in the job,
///    where processes on the same node have contiguous ranks, and
///
///  - another that is local to each node/locale, to support
///  - node-local barriers that other nodes do not participate in.
///
/// NOTE: there are a number of parameters that can be tuned for
/// specific use cases below.
///
/// TODO/NOTE: it is possible to issue send requests too fast for the
/// card. If that happens, this code will print an error and exit. I
/// have come other code that detects this condition and blocks/limits
/// the request rate instead; I'll integrate it if its needed.
///
class Verbs {
public:
    /// list of Verbs-capable devices
    ibv_device ** devices;
    int num_devices;
    /// info about chosen device
    std::vector<ibv_device*> virtual_devices;
    std::vector< const char *> device_names;
    std::vector<uint64_t> device_guids;
    std::vector<ibv_device_attr> devices_attribute;
    /// info about chosen port
    std::vector<uint8_t> ports;
    /// device context, used for most Verbs operations
    std::vector<ibv_context *> contexts;

    /// protection domain to go with context
    std::vector<ibv_pd *> protection_domains;

    std::vector<ibv_port_attr> ports_attribute;

    bool finalized;
    /// constants for initializing queues
    static const int send_completion_queue_depth = 8192;
    static const int recv_completion_queue_depth = 8192;
    static const int send_queue_depth = 8192;//16;         // how many operations per queue should we be able to enqueue at a time?
    static const int receive_queue_depth = 8192;//1;          // only need 1 if we're just using RDMA ops
    static const int scatter_gather_element_count = 2; // how many SGE's do we allow per operation?
    static const int max_inline_data = 16;             // message rate drops from 6M/s to 4M/s at 29 bytes
    static const int max_dest_rd_atomic = 16;          // how many outstanding reads/atomic ops are allowed? (remote end of qp, limited by card)
    static const int max_rd_atomic = 16;               // how many outstanding reads/atomic ops are allowed? (local end of qp, limited by card)
    static const int min_rnr_timer = 0x12;             // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
    static const int timeout = 0x12;                   // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
    static const int retry_count = 6;                  // from Mellanox RDMA-Aware Programming manual; probably don't need to touch
    static const int rnr_retry = 0;                    // from Mellanox RDMA-Aware Programming manual; probably don't need to touch

                                                       /// info about each endpoint (rank/process) in job
    struct Endpoint {
        uint16_t lid = -1;        // InfiniBand address of node. Remote Lid
        uint32_t qp_num = -1;     // Queue pair number on node (like IP port number)
        ibv_qp * queue_pair = NULL;
        int DeviceIdx = -1;
        int SocketIdx = -1;
        int RemoteMachineIdx = -1;
        int CoreIdx = -1;
        int RemoteQPIdx = -1;
        int CQIdx = -1;
        int Index = -1;
        uint32_t RemoteKey = 0;
        uint16_t LocalLid = 0;
    };
    std::vector<pthread_spinlock_t*> VSLs;
    int workerQPCount = -1;
    int psQPCount = -1;
    bool SmartPostNoLocking = false;
    double VerbsBitwidthReductionRatio = 1;
    std::vector<ibv_cq*> sendCompletionQueues;
    std::vector<ibv_cq*> receiveCompletionQueues;
    std::vector< Endpoint > endpoints;
    //these maps are used so that it is easy to swap out a partition scheme.
    //server/workerQP2CQIdx is private. could be potentially different for each server/worker for load balancing.
    std::vector<std::vector<int>> workerQPIdx2Keys;
    //we share global information. The following maps are the same across the entire system.
    std::vector<int> serverQPCounts;
    std::vector<int> serverKeyCounts;
    //map back
    std::vector<std::vector<int>> serverIDQPIdx2WkrQPIdx;
    std::vector<int> workerKey2QPIdx;
    std::vector<int> workerQP2SrvId;
    std::vector<int> workerQP2SrvQPIdx;
    std::vector<int*> QPStats;
    std::vector<std::vector<int>> core2CQIdxs;


    /// array of endpoints, one per rank


    int myId;
    int CoreCount = 0;
    int SocketCount = 0;
    /// Discover local Verbs-capable devices; choose one and prepare it for use.
    void initialize_device(const std::string desired_device_name, const int8_t desired_port) {
        //assign cores arbitrarily to sockets. CHange this when needed.
        SocketCount = numa_available();
        //No numa?
        if (SocketCount < 0) SocketCount = 1;
        else SocketCount = numa_num_task_nodes();
        CHECK(CoreCount > 0); //you have at least one core.
        std::vector<int> socket2CoreCnt(SocketCount, 0);
        //if (ps::Postoffice::Get()->van()->my_node().role == ps::Node::WORKER)
        //{
    //disable multi-socket
    //SocketCount = 1;
    //}
    //if(ps::Postoffice::Get()->van()->
        //CHECK(CoreCount >= SocketCount) << " you cannot have more sockets than there are cores on the system ";
    //int corePerSock = CoreCount / SocketCount;
	//dtermine what interfaces are blocked.
	auto blockedIFStr = getenv("PHUB_BLOCKED_INTERFACE");
	std::unordered_set<string> blockedIFSet;
	if(blockedIFStr != NULL)
	{
	    stringstream ss(blockedIFStr);
	    while(ss.good())
	    {
		string substr;
		getline(ss, substr, ',');
		blockedIFSet.insert(substr);
		LOG(INFO)<<"["<< ps::Postoffice::Get()->van()->my_node().id << "] Blocked Interface : " << substr;
	    }
	}
        if (ps::Postoffice::Get()->van()->my_node().role == ps::Node::SERVER)
        {
            for (int i = 0; i < CoreCount; i++)
            {
                //map this "cores" to a particular processor.
                //assign core to socket arbitrarily
                Core2SocketIdx.push_back(2 * i / CoreCount);
                socket2CoreCnt.at(2 * i / CoreCount)++;
                //printf("socket2CoreCnt[%d] = %d\n", 2*i/CoreCount, socket2CoreCnt.at(2 * i/CoreCount));
            }
        }
        else
        {
            //please assign everything to socket1.
            //CHECK(SocketCount == 2) << " worker has only 1 socket. change this to assign all cores to socket 0";
            for (int i = 0; i < CoreCount; i++)
            {
                if (desired_device_name == "mlx4_0")
                {
                    Core2SocketIdx.push_back(0);
                    socket2CoreCnt.at(0)++;
                }
                else
                {
                    Core2SocketIdx.push_back(1);
                    socket2CoreCnt.at(1)++;
                }
            }
        }
        // get device list
        devices = ibv_get_device_list(&num_devices);
        //make sure everyone has same number of cards.
        if (!devices) {
            std::cerr << "Didn't find any Verbs-capable devices!";
            exit(1);
        }
        // search for device
        for (size_t i = 0; i < num_devices; ++i) {
            // choose this device

            // open device context and get device attributes
            auto ctx = ibv_open_device(devices[i]);
            if (!ctx) {
                std::cerr << "Failed to get context for device = " << i << " with name = " << ibv_get_device_name(devices[i]) << "\n";
                exit(1);
            }
            ibv_device_attr dev_attr;
            int retval = ibv_query_device(ctx, &dev_attr);
            if (retval < 0) {
                perror("Error getting device attributes");
                exit(1);
            }
            auto breakDueToDesiredDeviceFound = false;
            for (int p = 1; p <= dev_attr.phys_port_cnt; p++)
            {
                auto name = devices[i];
		std::string nameStr(ibv_get_device_name(name));
                auto idx = nameStr.back() - '0';
		CHECK(num_devices <= 10) << "More than 10 devices. Check this.";
                CHECK(idx >= 0 && idx <= 9) << " this card (" << nameStr << ") has a name that does not conform to the format we understand.";
                auto socketId = idx <= 4 ? 0 : 1;

                ibv_port_attr port_attribute;
                retval = ibv_query_port(ctx, p, &port_attribute);
                if (retval < 0) {
                    perror("Error getting port attributes");
                    exit(1);
                }
		MigrateToNumaNode(socketId);
                auto protection_domain = ibv_alloc_pd(ctx);
                if (!protection_domain) {
                    std::cerr << "Error getting protection domain!\n";
                    exit(1);
                }
                //there are some dead links or dead ports - don't use them.
                if (port_attribute.link_layer != IBV_LINK_LAYER_INFINIBAND || port_attribute.state != IBV_PORT_ACTIVE)
                {
                    continue;
                }
                /*if((idx >= 4 && idx <= 4) || (idx >= 9 && idx <= 9))
                {
                    printf("Disabling idx=%d !\n", idx);
                    continue;
                    }*/
                    //worker assign everything to socketId=1
                if (ps::Postoffice::Get()->van()->my_node().role == ps::Node::WORKER)
                {
                    socketId = idx;
                    //numa 0 and numa 1
                }
                if ((desired_device_name.length() == 0 ||
		     (desired_device_name == ibv_get_device_name(name) && p == desired_port)) && socket2CoreCnt.at(socketId) > 0 && blockedIFSet.find(nameStr) == blockedIFSet.end())
                {
                    device_names.push_back(ibv_get_device_name(name));
                    device_guids.push_back(ntohll(ibv_get_device_guid(devices[i])));
                    // choose a port on the device and get port attributes
                    devices_attribute.push_back(dev_attr);
		    if(GetAddressNumaNode(protection_domain) != socketId)
		    {
			LOG(INFO) << " Protection domain for IF " << device_names.back() << " Port " << p << " is allocated on a remote numa node. target = " << socketId;
		    }

                    protection_domains.push_back(protection_domain);
                    contexts.push_back(ctx);
                    ports_attribute.push_back(port_attribute);
                    ports.push_back(p);
                    virtual_devices.push_back(devices[i]);
                    //printf("[%d] Device Discovery: %s @%d\n", myId, device_names.back(), ports.back());
                    //associate virtual devices to a socket.
                    //for now associate all cards to socket 0.
                    //change this for accurate mapping.
                    if (ps::Postoffice::Get()->van()->my_node().role == ps::Node::SERVER)
                    {
                        Device2SocketIdx.push_back(socketId);
                        //printf("[Interface Assign] PHUB assigned interface %s to socket %d. There are %d cores in socket.\n", nameStr.c_str(), socketId, socket2CoreCnt.at(socketId));
                    }
                    else
                    {
                        //shovel it to socket 1 for now.
                        Device2SocketIdx.push_back(socketId);
                    }
                    if (desired_device_name == ibv_get_device_name(name))
                    {
                        //use only this device.
                        printf("[%d][Desired Device Found] Desired Device Found : %s, port : %d \n",
                            ps::Postoffice::Get()->van()->my_node().id,
                            device_names.back(), p);
                        breakDueToDesiredDeviceFound = true;
                        break;
                    }
                }
            }
            if (breakDueToDesiredDeviceFound)
            {
                break;
            }
        }
        CHECK(virtual_devices.size() > 0) << "Node Id = [" << myId << "] found no device";
        //assign arbitrarily remote to device idx.
        auto remoteCnt = ps::Postoffice::Get()->van()->my_node().role == ps::Node::WORKER ? ps::Postoffice::Get()->num_servers() : ps::Postoffice::Get()->num_workers();
        for (auto rmid = 0; rmid < remoteCnt; rmid++)
        {
            Remote2VirtualDeviceIdx.push_back(rmid % virtual_devices.size());
        }


        //bug fix. avoid blocking
        /*for(size_t i = 1; i < virtual_devices.size(); i++)
        {
            device_names.at(i) = device_names.at(0);
            virtual_devices.at(i) = virtual_devices.at(0);
            device_guids.at(i) = device_guids.at(0);
            devices_attribute.at(i) = devices_attribute.at(0);
            ports_attribute.at(i) = ports_attribute.at(0);
            ports.at(i) = ports.at(0);
            contexts.at(i) = contexts.at(0);
            protection_domains.at(i) = protection_domains.at(0);
            }*/
        CHECK(protection_domains.size() == contexts.size());
        CHECK(protection_domains.size() == device_names.size());
        CHECK(protection_domains.size() == virtual_devices.size());
        CHECK(protection_domains.size() == device_guids.size());
        CHECK(protection_domains.size() == devices_attribute.size());
        CHECK(protection_domains.size() == ports_attribute.size());
        CHECK(protection_domains.size() == ports.size());
        CHECK(protection_domains.size() == Device2SocketIdx.size());
        //printf("[%d] initialize_device done\n", myId);
    }

    int GetSQDepth()
    {
        return send_queue_depth - 1;
    }

    /// release resources on device in preparation for shutting down
    void finalize_device() {
        //  print_stacktrace();
        if (finalized) return;
        //strawman lock
        for (auto endpoint : endpoints) {
            if (endpoint.queue_pair) {
                int retval = ibv_destroy_qp(endpoint.queue_pair);
                if (retval < 0) {
                    perror("Error destroying queue pair");
                    exit(1);
                }
                endpoint.queue_pair = nullptr;
            }
            endpoints.clear();
        }

        for (size_t i = 0; i < sendCompletionQueues.size(); i++)
        {
            auto completion_queue = sendCompletionQueues[i];
            //printf("[%d]destorying send completion_queue[%d]!\n",myId,i);

            if (completion_queue) {
                int retval = ibv_destroy_cq(completion_queue);
                if (retval < 0) {
                    perror("Error destroying completion queue");
                    exit(1);
                }
                //completion_queue = nullptr;
            }
        }

        for (size_t i = 0; i < receiveCompletionQueues.size(); i++)
        {
            auto completion_queue = receiveCompletionQueues[i];
            //printf("[%d]destorying recv completion_queue[%d]!\n",myId,i);
            if (completion_queue) {
                int retval = ibv_destroy_cq(completion_queue);
                if (retval < 0) {
                    printf("Error destroying completion queue");
                    exit(1);
                }
                //completion_queue = nullptr;
            }
        }
        std::unordered_set<ibv_context*> freedDevices;
        for (size_t i = 0; i < virtual_devices.size(); i++)
        {
            auto protection_domain = protection_domains.at(i);
            if (protection_domain) {
                int retval = ibv_dealloc_pd(protection_domain);
                if (retval < 0) {
                    perror("Error deallocating protection domain");
                    exit(1);
                }
                protection_domain = nullptr;
            }
            ibv_context* context = contexts.at(i);
            //some devices have multiple ports, and they are virtualized as many devices.
            //free them only once.
            if (context && freedDevices.find(context) == freedDevices.end()) {
                int retval = ibv_close_device(context);
                if (retval < 0) {
                    perror("Error closing device context");
                    exit(1);
                }
                freedDevices.insert(context);
                //context = nullptr;
            }
            auto device = devices[i];
            if (device) {
                device = nullptr;
            }
        }

        if (devices) {
            ibv_free_device_list(devices);
            devices = nullptr;
        }
        finalized = true;
    }

    /// set up queue pairs for RDMA operations
    //each worker talk to each server.
    //key sizes are VIRTUAL KEYS.
    //each CQ maps to a single core.
    //each CQ contains 1 QP, and each QP contains 
    //numCQs: CQ per machine.
    //machines: This is computed in the body of this call.
    bool DirectConnect = true;
    std::vector<int> Core2SocketIdx;
    std::vector<int> Remote2VirtualDeviceIdx;
    std::vector<int> Device2SocketIdx;
    std::vector<int> CQ2DeviceIdx;
    std::vector<ibv_mr*> DeviceMemoryRegions;
    //this is per worker/server information.
    std::vector<float> workerQPSizes;
    std::vector<float> serverQPSizes;
    std::vector<int> approximateSetPartition(std::vector<float>& sizes, int setCount)
    {
        if (setCount == 0) return std::vector<int>();
        std::vector<std::pair<int, float>> sizePaired(sizes.size());
        for (size_t i = 0; i < sizes.size(); i++)
        {
            sizePaired[i].first = i;
            sizePaired[i].second = sizes[i];
        }
        std::sort(sizePaired.begin(), sizePaired.end(), [](const std::pair<int, int> &left, const std::pair<int, int> &right)
        {
            return left.second > right.second;
        });
        std::vector<float> accumulator(setCount, 0.0f);
        std::vector<int> result(sizes.size());
        //printf("sizes count = %d, set = %d\n", sizes.size(),  setCount);
        for (size_t i = 0; i < sizes.size(); i++)
        {
            /*auto it = std::min_element(accumulator.begin(), accumulator.end());
            auto idx = it - accumulator.begin();
            result.at(sizePaired[i].first) = idx;
        accumulator.at(idx) += sizes[i];*/
        //find the minimum in accumulator.
            int minIdx = 0;
            for (size_t acc = 0; acc < accumulator.size(); acc++)
            {
                if (accumulator.at(acc) < accumulator.at(minIdx))
                {
                    minIdx = acc;
                }
            }
            result.at(sizePaired[i].first) = minIdx;
            accumulator.at(minIdx) += sizes[i];
            //printf("assigned %d = %f to set %d, set is currently %f\n", sizePaired[i].first, sizePaired[i].second, minIdx, accumulator.at(minIdx));
                //assign this to the minimum buckets.
        }
        return result;
    }

    void initialize_queue_pairs(
        std::unordered_map<int, int>& keySizes) {
        //    printf("[%d] 1\n", myId);

                //first, partition keys into different qps

                // allocate storage for each endpoint in job
        CHECK(keySizes.size() >= workerQPCount) << " more qp = " << workerQPCount << " than keys = " << keySizes.size() << " specified.";
        auto myRole = ps::Postoffice::Get()->van()->my_node().role;
        CHECK(myRole == ps::Node::Role::SERVER || myRole == ps::Node::Role::WORKER);
        int MyID = ps::Postoffice::Get()->van()->my_node().id;
        int myRank = ps::Postoffice::Get()->my_rank();
        auto numWorkers = ps::Postoffice::Get()->num_workers();
        auto numServers = ps::Postoffice::Get()->num_servers();
        auto MachineCount = 0;
        std::vector<int> remoteNodes;
        //first, we all need to know how keys are mapped to QPs.
        //change differeny maps to try different assignment strategy.

        //create a vector for set partitioning.
        std::vector<float> keySizeVector(keySizes.size());
        for (size_t i = 0; i < keySizes.size(); i++)
        {
            keySizeVector.at(i) = keySizes[i];
        }

        int psTotalQPCount = 0;
        workerKey2QPIdx = approximateSetPartition(keySizeVector, workerQPCount);

        //map back of the previous.
        workerQPIdx2Keys.resize(workerQPCount);
        workerQPSizes.resize(workerQPCount);
        //assuming continuous keys.
        for (size_t i = 0; i < keySizes.size(); i++)
        {
            //currently this is just rotation.
            auto wQPidx = workerKey2QPIdx[i];
            //printf("[%d] key = %d is assigned qp = %d\n", myRank, i, wQPidx);
            workerQPIdx2Keys.at(wQPidx).push_back(i);
            workerQPSizes.at(wQPidx) += keySizes[i];
        }
        //now assign each qp to a server.
        //now for each server, my qp[i] maps to which qp idx in server?
        //server QPIdxs is a ticketer.
        serverQPCounts.resize(numServers);
        serverKeyCounts.resize(numServers);
        workerQP2SrvId.resize(workerQPCount);
        workerQP2SrvQPIdx.resize(workerQPCount);
        //these maps are seldom used
        serverIDQPIdx2WkrQPIdx.resize(numServers);
        for (size_t i = 0; i < workerQPCount; i++)
        {
            //since these queue pairs are roughly balanced, 
            //just modulo them.
            workerQP2SrvId[i] = i % numServers;
            //printf("[%d] view of qp = %d is assigned to server %d\n", myRank, i, workerQP2SrvId[i]);
            int id = serverQPCounts[workerQP2SrvId[i]];
            workerQP2SrvQPIdx[i] = id;
            serverIDQPIdx2WkrQPIdx[workerQP2SrvId[i]].push_back(i);
            serverQPCounts[workerQP2SrvId[i]]++;

        }
        //populate server key counts.
        for (size_t i = 0; i < keySizes.size(); i++)
        {
            auto qpIdx = workerKey2QPIdx[i];
            auto srvId = workerQP2SrvId[qpIdx];
            serverKeyCounts[srvId]++;
        }
        psQPCount = 0; //workerQPCount / numServers;
        /*for (size_t i = 0; i < serverQPCounts.size(); i++)
        {
      printf("[%d][%d] view on key from server/worker %d is : %d, key count = %d\n", MyID, myRole, i, serverQPCounts[i], serverKeyCounts[i]);
      }*/
      //printf("[%d] psQPCount = %d\n", ps::Postoffice::Get()->van()->my_node().id, psQPCount);

        std::vector<std::vector<int>> socket2CoreIdx(SocketCount);
        std::vector<int> socketTicketer(SocketCount);
        for (size_t i = 0; i < CoreCount; i++)
        {
            socket2CoreIdx.at(Core2SocketIdx.at(i)).push_back(i);
        }
        if (myRole == ps::Node::Role::SERVER)
        {
            serverQPSizes.resize(serverQPCounts.at(ps::Postoffice::Get()->my_rank()));
            //calculate per worker
            for (size_t i = 0; i < workerQP2SrvId.size(); i++)
            {
                if (workerQP2SrvId[i] == ps::Postoffice::Get()->my_rank())
                {
                    psQPCount++;
                    serverQPSizes.at(workerQP2SrvQPIdx.at(i)) += workerQPSizes.at(i);
                }
            }
            CHECK(psQPCount == serverQPSizes.size());
            MachineCount = numWorkers;

            remoteNodes = ps::Postoffice::Get()->GetNodeIDs(ps::kWorkerGroup);
            //each worker MUST agree how QPs are partitioned into different servers.
            //how many QPs do I receive?
            psTotalQPCount = psQPCount* MachineCount;
            QPStats.resize(psTotalQPCount, NULL);
            //if not PHUB, initialize QPStats.
            if (ps::Postoffice::Get()->van()->VanType() != "pshub")
            {
                for (int i = 0; i < QPStats.size(); i++)
                {
                    //workers need to initialize QPStats during setup.
                    QPStats[i] = new int;
                    *QPStats[i] = GetSQDepth();
                }
            }

            //multiply per worker qp assignment by the number of workers.
            //now partition these queue pairs into different CQs.
            //We now need to understand the size of each qp, then partition them equally to CQs for balancing.
            //this produces QP2CQIdx
            endpoints.resize(psTotalQPCount);
            VSLs.resize(psTotalQPCount);
            for (size_t i = 0; i < psTotalQPCount; i++)
            {
                VSLs[i] = new pthread_spinlock_t;
                CHECK(0 == pthread_spin_init(VSLs[i], PTHREAD_PROCESS_PRIVATE)) << myId << " could not initialize spinlock " << i;
            }
            //map QP to a device.
            //if i am a server, depending on assignment policy, i:
            //this is through a switch.
            //i am free to assign QP to arbitrary device, provided that:
            //*All QPS corresponding to the same key from each worker is through the same device.
            //attempt to balance qpairs to vdev

            if (DirectConnect == false)
            {
                auto switchConnectQP2Dev = approximateSetPartition(serverQPSizes, virtual_devices.size());
                for (size_t i = 0; i < psQPCount; i++)
                {
                    auto devId = switchConnectQP2Dev.at(i);
                    auto socketId = Device2SocketIdx.at(devId);
                    //assign to a processor on the socket. This is fine because we have balanced device workload.

                    auto coreIdxInSocket = socketTicketer.at(socketId) % socket2CoreIdx.at(socketId).size();
                    auto coreIdx = socket2CoreIdx.at(socketId).at(coreIdxInSocket);
                    for (size_t mid = 0; mid < numWorkers; mid++)
                    {
                        //            printf("[%d] 2.43 endpoints.size() = %d, mid * psQPCount + i = %d, devId = %d, dev2socket.size() = %d, rnk = %d, serverIDQP.size() = %d\n", myId, endpoints.size(), mid * psQPCount + i, devId, Device2SocketIdx.size(),myRank, serverIDQPIdx2WkrQPIdx.at(myRank).size() );
                        //if (DirectConnect == true)
                        //{
                        //    //override you. devId is the remote worker's devid.
                        //    devId = Remote2VirtualDeviceIdx.at(mid);
                        //    socketId = Device2SocketIdx.at(devId);
                        //    //assign to a processor on the socket.
                        //    coreIdxInSocket = socketTicketer.at(socketId) % socket2CoreIdx.at(socketId).size();
                        //    coreIdx = socket2CoreIdx.at(socketId).at(coreIdxInSocket);
                        //    //switch cores.
                        //}

                        endpoints.at(mid * psQPCount + i).DeviceIdx = devId;
                        endpoints.at(mid * psQPCount + i).LocalLid = ports_attribute.at(devId).lid;
                        endpoints.at(mid * psQPCount + i).SocketIdx = Device2SocketIdx.at(devId);
                        //assign all endpoints containing that group of keys to the same processor for all workers.
                        endpoints.at(mid * psQPCount + i).CoreIdx = coreIdx;
                        endpoints.at(mid * psQPCount + i).RemoteMachineIdx = mid;
                        endpoints.at(mid * psQPCount + i).RemoteQPIdx = serverIDQPIdx2WkrQPIdx.at(myRank).at(i);
                        CHECK(Core2SocketIdx.at(coreIdx) == Device2SocketIdx.at(devId));

                    }
                    //if (DirectConnect == false)
                    //{
                         //the same core is in charge of all queue pairs (of that group of keys) at that index in server.
                    socketTicketer.at(socketId)++;
                    //else
                    //{
                    //    //when direct connect is enabled, we assume all sockets are utilized by the worker machines.
                    //    //increment all socketTicketers.
                    //    for (size_t sid = 0; sid < SocketCount; sid++)
                    //    {
                    //        socketTicketer.at(sid)++;
                    //    }
                    //}
                }
            }
            else
            {
                //first calculate per socket connections.
                std::vector<std::vector<int>> socket2QPs(SocketCount);
                std::vector<std::vector<float>> socketQPSizes(SocketCount);
                for (int i = 0; i < SocketCount; i++)
                {
                    if (socket2CoreIdx.at(i).size() == 0) continue;//this socket does not have any core.
                    auto perSocketPSQPBalance = approximateSetPartition(serverQPSizes, socket2CoreIdx.at(i).size());
                    for (int qp = 0; qp < psQPCount; qp++)
                    {
                        //socket i
                        for (int mid = 0; mid < MachineCount; mid++)
                        {
                            auto devId = Remote2VirtualDeviceIdx.at(mid);
                            auto sid = Device2SocketIdx.at(devId);
                            if (sid != i)
                            {
                                continue;
                            }
                            auto coreIdx = socket2CoreIdx.at(i).at(perSocketPSQPBalance.at(qp));
                            auto eqpidx = mid * psQPCount + qp;
                            endpoints.at(eqpidx).DeviceIdx = devId;
                            endpoints.at(eqpidx).LocalLid = ports_attribute.at(devId).lid;
                            endpoints.at(eqpidx).SocketIdx = sid;
                            endpoints.at(eqpidx).CoreIdx = coreIdx;
                            endpoints.at(eqpidx).RemoteMachineIdx = mid;
                            endpoints.at(eqpidx).RemoteQPIdx = serverIDQPIdx2WkrQPIdx.at(myRank).at(qp);
                        }
                    }
                }
                //direct connect.
            }


            //maps a sockect id to a list of QPs.
            ////i need to know who ispolling who.
            ////maps a socket id to a list of qps.
            ////within each socket, i need to make sure that cores on the same socket touches qps on the same socket.
            ////this is a worker, a key is mapped to a single QP and thus a single device.

        }
        else
        {
            MachineCount = ps::Postoffice::Get()->num_servers();
            remoteNodes = ps::Postoffice::Get()->GetNodeIDs(ps::kServerGroup);
            //is worker, get all servers
            endpoints.resize(workerQPCount);
            QPStats.resize(workerQPCount, NULL);
            for (int i = 0; i < QPStats.size(); i++)
            {
                //workers need to initialize QPStats during setup.
                QPStats[i] = new int;
                *QPStats[i] = GetSQDepth();
            }
            //qp2CQIndex.
            VSLs.resize(workerQPCount);
            for (size_t i = 0; i < workerQPCount; i++)
            {
                VSLs[i] = new pthread_spinlock_t;
                CHECK(0 == pthread_spin_init(VSLs[i], PTHREAD_PROCESS_PRIVATE)) << myId << " could not initialize spinlock " << i;
            }
            //if i am a worker, i am free to map a QP to an arbitrary device.
            for (size_t i = 0; i < endpoints.size(); i++)
            {
                auto devId = i % virtual_devices.size();
                endpoints.at(i).DeviceIdx = devId;
                auto socketId = Device2SocketIdx.at(devId);
                endpoints.at(i).SocketIdx = socketId;
                auto ridx = workerQP2SrvId.at(i);
                endpoints.at(i).RemoteMachineIdx = ridx;
                endpoints.at(i).RemoteQPIdx = myRank * serverQPCounts.at(ridx) + workerQP2SrvQPIdx.at(i);
                //i need to assign core index.
        //SIGFPE
                auto coreIdxInSocket = socketTicketer.at(socketId)++ % socket2CoreIdx.at(socketId).size();
                auto coreIdx = socket2CoreIdx.at(socketId).at(coreIdxInSocket);
                endpoints.at(i).CoreIdx = coreIdx;
                endpoints.at(i).LocalLid = ports_attribute.at(devId).lid;
            }
            //        printf("[%d] 2.4\n", myId);


        }

        //calculate how many completion queues are needed
        //create core/device/QPs.
        std::vector<std::unordered_map<int, std::vector<int>>> coreDevQps(CoreCount);
        for (size_t i = 0; i < endpoints.size(); i++)
        {
            endpoints.at(i).Index = i;
            auto coreIdx = endpoints.at(i).CoreIdx;
            auto devId = endpoints.at(i).DeviceIdx;
            coreDevQps.at(coreIdx)[devId].push_back(i);
        }

        int CQCounter = 0;
        core2CQIdxs.resize(CoreCount);
        for (size_t i = 0; i < CoreCount; i++)
        {
            for (auto& devQpPairs : coreDevQps.at(i))
            {
                //your device id, also your CQIndex.
                for (auto qpIdx : devQpPairs.second)
                {
                    endpoints.at(qpIdx).CQIdx = CQCounter;
                }
                //increment CQ each time a core changes or its associated card changes.
                core2CQIdxs.at(i).push_back(CQCounter);
                CQ2DeviceIdx.push_back(devQpPairs.first);
                CQCounter++;
            }
        }

        // create shared completion queue

        sendCompletionQueues.resize(CQCounter);
        receiveCompletionQueues.resize(CQCounter);
        //create a CQ to Socket mapping.
        vector<vector<int>> Socket2CQ(SocketCount);
        for (size_t i = 0; i < CQCounter; i++)
        {
            auto dev = CQ2DeviceIdx.at(i);
            auto sock = Device2SocketIdx.at(dev);
            Socket2CQ.at(sock).push_back(i);
        }
        //now allocate these structures on different sockets.
        for (size_t sockId = 0; sockId < Socket2CQ.size(); sockId++)
        {
	    //if(myRole == ps::Node::SERVER)
	    //{
		MigrateToNumaNode(sockId);
		//}
            for (size_t sockSeq = 0; sockSeq < Socket2CQ.at(sockId).size(); sockSeq++)
            {
                //i is effective index.
                auto i = Socket2CQ.at(sockId).at(sockSeq);
                auto devId = CQ2DeviceIdx.at(i);
                sendCompletionQueues[i] = ibv_create_cq(contexts.at(devId),
                    send_completion_queue_depth,
                    NULL,  // no user context
                    NULL,  // no completion channel 
		    0);   // no completion channel vector
		if(GetAddressNumaNode(sendCompletionQueues[i]) != sockId)
		{
		    LOG(INFO) << " sendCompletionqueue on sock = " << sockId << " seq = " << i << " failed to allocate on same socket";
		}
                receiveCompletionQueues[i] = ibv_create_cq(contexts.at(devId),
                    recv_completion_queue_depth,
                    NULL,  // no user context
                    NULL,  // no completion channel 
                    0);   // no completion channel vector
		if(GetAddressNumaNode(receiveCompletionQueues[i]) != sockId)
		{
		    LOG(INFO) << " recvCompletionqueue on sock = " << sockId << " seq = " << i << " failed to allocate on same socket";
		}

                if (sendCompletionQueues[i] == NULL || receiveCompletionQueues[i] == NULL) {
                    std::cerr << "Error creating completion queue!\n";
                    exit(1);
                }
            }
        }
        //for (size_t i = 0; i < CQCounter; i++)
        //{
        //    auto devId = CQ2DeviceIdx.at(i);
        //    sendCompletionQueues[i] = ibv_create_cq(contexts.at(devId),
        //        send_completion_queue_depth,
        //        NULL,  // no user context
        //        NULL,  // no completion channel 
        //        0);   // no completion channel vector
        //    receiveCompletionQueues[i] = ibv_create_cq(contexts.at(devId),
        //        recv_completion_queue_depth,
        //        NULL,  // no user context
        //        NULL,  // no completion channel 
        //        0);   // no completion channel vector
        //    if (sendCompletionQueues[i] == NULL || receiveCompletionQueues[i] == NULL) {
        //        std::cerr << "Error creating completion queue!\n";
        //        exit(1);
        //    }
        //}
        //    printf("[%d] 2.6\n", myId);


        //create queue pair for each endpoint
        //first, create QP 2 Socket map.
        std::vector<std::vector<int>> QP2SocketIdx(SocketCount);
        for (size_t i = 0; i < endpoints.size(); i++)
        {
            auto& ep = endpoints[i];
            auto sock = ep.SocketIdx;
            QP2SocketIdx.at(sock).push_back(i);
        }
        //now migrate to different numa to create QPs
        for (size_t sockId = 0; sockId < QP2SocketIdx.size();sockId++)
        {
            MigrateToNumaNode(sockId);
            for (size_t sockSeq = 0;sockSeq < QP2SocketIdx.at(sockId).size(); ++sockSeq)
            {
                auto i = QP2SocketIdx.at(sockId).at(sockSeq);
                // create queue pair for this endpoint
                ibv_qp_init_attr init_attributes;
                std::memset(&init_attributes, 0, sizeof(ibv_qp_init_attr));
                int cqIndex = endpoints.at(i).CQIdx;
                // use shared completion queue
                init_attributes.send_cq = sendCompletionQueues.at(cqIndex);
                init_attributes.recv_cq = receiveCompletionQueues.at(cqIndex);
                //make sure we're using the same device.
                CHECK(endpoints.at(i).DeviceIdx == CQ2DeviceIdx.at(cqIndex));
                // use "reliable connected" model in order to support RDMA atomics
                init_attributes.qp_type = IBV_QPT_RC;

                // only issue send completions if requested
                init_attributes.sq_sig_all = 0;

                // set queue depths and WR parameters accoring to constants declared earlier
                init_attributes.cap.max_send_wr = send_queue_depth;
                init_attributes.cap.max_recv_wr = receive_queue_depth;
                init_attributes.cap.max_send_sge = scatter_gather_element_count;
                init_attributes.cap.max_recv_sge = scatter_gather_element_count;
                init_attributes.cap.max_inline_data = max_inline_data;
                auto devId = endpoints.at(i).DeviceIdx;
                // create queue pair
            //printf("[%d] devid = %d, cq = %d, ep = %d, sendCQ = %p, context = %p\n", MyID, devId, cqIndex, i, sendCompletionQueues.at(cqIndex), protection_domains.at(devId)->context);
                endpoints[i].queue_pair = ibv_create_qp(protection_domains.at(devId), &init_attributes);
		//CHECK(GetAddressNumaNode(endpoints[i].queue_pair) == sockId) << " queue pair allcoation on sock=  " << sockId << " seq = " << i << " is on a wrong socket";
                if (endpoints[i].queue_pair == NULL) {
                    perror("QP Creation:");
                    printf("[%d] Error creating queue pair %d\n. Errno = %d", MyID, i, errno);
                    exit(1);
                }
            }
        }
        printf("[%d] queue pair creation done.\n", myId);
    }


    /// release resources on device in preparation for shutting down

    void connect_queue_pairs()
    {
        auto allocator = PHubAllocator::Get();
        DeviceMemoryRegions.resize(virtual_devices.size());
        //register memory addresses with Verbs.
        for (size_t i = 0; i < virtual_devices.size(); i++)
        {
            auto socketIdx = Device2SocketIdx.at(i);
            size_t len = 0;
            auto base = allocator->GetStartAddress(socketIdx, len);
            auto mbr = DeviceMemoryRegions.at(i) = register_memory_region(base, len, i);
            //printf("[%d] mbr.rkey = %llu, mbr.lkey = %llu\n", myId, mbr->rkey, mbr->lkey);
        }


        std::vector<int> remoteNodes;
        int MachineCount;
        auto myRole = ps::Postoffice::Get()->van()->my_node().role;
        if (myRole == ps::Node::Role::SERVER)
        {
            MachineCount = ps::Postoffice::Get()->num_workers();
            remoteNodes = ps::Postoffice::Get()->GetNodeIDs(ps::kWorkerGroup);
        }
        else
        {
            MachineCount = ps::Postoffice::Get()->num_servers();
            remoteNodes = ps::Postoffice::Get()->GetNodeIDs(ps::kServerGroup);
            //is worker, get all servers
        }
        // wait until all ranks have created their queue pairs
        ps::Postoffice::Get()->Barrier(ps::kWorkerGroup + ps::kServerGroup);

        CHECK(allocator->IsInitialized());

        //block until all have received lids.
        //this call must be called by the same thread that executes Van::Start();
        auto thisVan = (ps::Postoffice::Get()->van());
        int MyID = ps::Postoffice::Get()->van()->my_node().id;
        ps::Node::Role MyRole = ps::Postoffice::Get()->van()->my_node().role;
        //Summarize();
        int QPCount = MyRole == ps::Node::SERVER ? psQPCount : workerQPCount;

        for (size_t i = 0; i < remoteNodes.size(); i++)
        {
            Endpoint ep;
            std::vector<uint32_t> myQPBundles;
            std::vector<uintptr_t> myKeyAddrs;
            std::vector<uintptr_t> myMetaAddrs;
            std::vector<uint16_t> myLids;
            std::vector<uint32_t> myRKeys;
            //populate QP Bundles.
            auto mid = ps::Postoffice::Get()->IDtoRank(remoteNodes[i]);

            for (int qid = 0; qid < QPCount; qid++)
            {
                if (MyRole == ps::Node::WORKER)
                {
                    //only send relevant QPs
                    if (mid == workerQP2SrvId[qid])
                    {
                        myQPBundles.push_back(endpoints[qid].queue_pair->qp_num);
                        myLids.push_back(endpoints[qid].LocalLid);
                        //auto socketId = endpoints.at(qid).SocketIdx;
                        auto devId = endpoints.at(qid).DeviceIdx;
                        auto rkey = DeviceMemoryRegions.at(devId)->rkey;
                        myRKeys.push_back(rkey);
                    }
                }
                else
                {
                    auto eQpid = mid * QPCount + qid;
                    myQPBundles.push_back(endpoints[eQpid].queue_pair->qp_num);
                    myLids.push_back(endpoints[eQpid].LocalLid);
                    //auto socketId = endpoints.at(eQpid).SocketIdx;
                    auto devId = endpoints.at(eQpid).DeviceIdx;
                    auto rkey = DeviceMemoryRegions.at(devId)->rkey;
                    myRKeys.push_back(rkey);
                }

            }
            //CHECK(myQPBundles.size() == serverQPCounts[mid]) << "["<<MyID<<"] disagrees on how many QPs to send. GlobalKnowledge: " << serverQPCounts[mid] << " LocalKnowledge: " << myQPBundles.size();
            //deal with kvAddr and metaAddr
            //key sizes and meta sizes are IDENTIFCAL on both sides.
            for (size_t j = 0; j < allocator->KeyCount; j++)
            {
                size_t len;
                if (MyRole == ps::Node::WORKER)
                {
                    auto socketIdx = Helper_Worker_GetEndpointFromKey(j).SocketIdx;
                    myKeyAddrs.push_back((uintptr_t)allocator->WorkerKVBuffer(j, socketIdx, len));
                    myMetaAddrs.push_back((uintptr_t)allocator->WorkerMetaBuffer(j, socketIdx, len));
                }
                else
                {
                    //is this key for me?
                    //if not, use 0.
                    auto qp = workerKey2QPIdx.at(j);
                    auto myKey = workerQP2SrvId.at(qp) == ps::Postoffice::Get()->my_rank();
                    if (myKey)
                    {
                        auto socketIdx = Helper_Server_GetEndpointFromKey(j, i).SocketIdx;
                        myKeyAddrs.push_back((uintptr_t)allocator->PHUBReceiveKVBuffer(j, i, socketIdx, len));
                        myMetaAddrs.push_back((uintptr_t)allocator->PHUBReceiveMetaBuffer(j, i, socketIdx, len));

                    }
                    else
                    {
                        myKeyAddrs.push_back(NULL);
                        myMetaAddrs.push_back(NULL);
                    }
                }
            }
            Message msg;
            //memset(&msg, 0, sizeof(Message));
            msg.meta.control.cmd = Control::INFINIBANDXCHGQPS;
            msg.meta.recver = remoteNodes[i];
            msg.meta.request = false;
            msg.meta.push = false;
            msg.meta.simple_app = false;
            msg.meta.timestamp = thisVan->GetTimestamp();
            msg.meta.control.msg_sig = myQPBundles.size();
            msg.AddData(SArray<uint32_t>(myQPBundles));
            msg.AddData(SArray<uint16_t>(myLids));
            msg.AddData(SArray<uint32_t>(myRKeys));
            msg.AddData(SArray<uintptr_t>(myKeyAddrs));
            msg.AddData(SArray<uintptr_t>(myMetaAddrs));
            PS_VLOG(1) << Postoffice::Get()->van()->my_node().id << " is  broadcasting its qps to " << remoteNodes[i];
            /*for(auto id : myQPBundles)
            {
            printf("[%d]QPBroadcast[%d]:%d\n",MyID,i,id);

            }*/
            ps::Postoffice::Get()->van()->Send(msg);
            PS_VLOG(1) << Postoffice::Get()->van()->my_node().id << " has broadcast qps to " << remoteNodes[i];
        }

        thisVan->WaitForQPExchanges(remoteNodes.size());
        PS_VLOG(1) << "Rank: " << MyID << " has exchanged its QP with all " << remoteNodes.size() << " participants";

        //{ // exchange queue pair numbers
        //  // first, prepare contiguous list of my QP numbers
        //    std::vector< uint32_t > my_qp_nums(MachineCount * workerQPCount);
        //    for (int i = 0; i < m.size; ++i) {
        //        my_qp_nums[i] = endpoints[i].queue_pair->qp_num;
        //    }

        //    // now, gather list of remote QP numbers
        //    std::vector< uint32_t > remote_qp_nums(m.size);
        //    MPI_CHECK(MPI_Alltoall(&my_qp_nums[0], 1, MPI_UINT32_T,
        //        &remote_qp_nums[0], 1, MPI_UINT32_T,
        //        m.main_communicator_));
        //    for (int i = 0; i < m.size; ++i) {
        //        endpoints[i].qp_num = remote_qp_nums[i];
        //    }
        //}

        // once everybody's done with that, we can go connect each one of our queues
        ps::Postoffice::Get()->Barrier(ps::kWorkerGroup + ps::kServerGroup);

        // Connect all our queue pairs: move queues through INIT, RTR, and RTS
        for (size_t i = 0; i < endpoints.size(); ++i) {
            //printf("[%d]initializing endpoint[%d]\n", thisVan->my_node().id, i);
            auto devId = endpoints.at(i).DeviceIdx;
            CHECK(endpoints[i].queue_pair != NULL);
            ibv_qp_attr attributes;
            std::memset(&attributes, 0, sizeof(attributes));

            // move to INIT
            attributes.qp_state = IBV_QPS_INIT;
            attributes.port_num = ports.at(devId);
            attributes.pkey_index = 0;
            attributes.qp_access_flags = (IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_ATOMIC);
            int retval = ibv_modify_qp(endpoints[i].queue_pair, &attributes,
                IBV_QP_STATE |
                IBV_QP_PKEY_INDEX |
                IBV_QP_PORT |
                IBV_QP_ACCESS_FLAGS);
            if (retval < 0) {
                printf("[%d]ibv_modify_qp[%d] phase1 retVal=%d\n", thisVan->my_node().id, i, retval);
                perror("Error setting queue pair to INIT");
                exit(1);
            }


            /// in theory, we need to post an empty receive WR to proceed, but
            /// when we're doing RDMA-only stuff it seems to work without one.
            // bare_receives[i].wr_id = 0xdeadbeef;
            // bare_receives[i].next = NULL;
            // bare_receives[i].sg_list = NULL;
            // bare_receives[i].num_sge = 0;
            // post_receive( i, &bare_receives[i] );

            // move to RTR
            std::memset(&attributes, 0, sizeof(attributes));
            attributes.qp_state = IBV_QPS_RTR;
            attributes.path_mtu = ports_attribute.at(devId).active_mtu;
            attributes.dest_qp_num = endpoints[i].qp_num;
            attributes.rq_psn = 0;
            attributes.max_dest_rd_atomic = max_dest_rd_atomic;
            attributes.min_rnr_timer = min_rnr_timer;
            attributes.ah_attr.is_global = 0;
            attributes.ah_attr.dlid = endpoints[i].lid;
            attributes.ah_attr.sl = 0;
            attributes.ah_attr.src_path_bits = 0;
            attributes.ah_attr.port_num = ports.at(devId);
            retval = ibv_modify_qp(endpoints[i].queue_pair, &attributes,
                IBV_QP_STATE |
                IBV_QP_AV |
                IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER);
            if (retval < 0) {
                perror("Error setting queue pair to RTR");
                exit(1);
            }
            //printf("[%d]ibv_modify_qp[%d] phase2\n", thisVan->my_node().id, i);
            // move to RTS
            std::memset(&attributes, 0, sizeof(attributes));
            attributes.qp_state = IBV_QPS_RTS;
            attributes.timeout = timeout;
            attributes.retry_cnt = retry_count;
            attributes.rnr_retry = rnr_retry;
            attributes.sq_psn = 0;
            attributes.max_rd_atomic = max_rd_atomic;
            retval = ibv_modify_qp(endpoints[i].queue_pair, &attributes,
                IBV_QP_STATE |
                IBV_QP_TIMEOUT |
                IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN |
                IBV_QP_MAX_QP_RD_ATOMIC);
            if (retval < 0) {
                perror("Error setting queue pair to RTR");
                exit(1);
            }
            //printf("[%d]ibv_modify_qp[%d] phase3\n", thisVan->my_node().id, i);
        }

        // wait for everybody to have connected everything before proceeding
        ps::Postoffice::Get()->Barrier(ps::kWorkerGroup + ps::kServerGroup);
        PS_VLOG(1) << "node " << thisVan->my_node().id << " has finished setting up infiniband!";
    }

    /// post a send request to a remote rank
    /// post a send request to a remote rank
    void post_send(int remote_rank, ibv_send_wr * wr) {
        ibv_send_wr * bad_wr = nullptr;

        //printf("[%d] attempting to post send wr to endpoint %d ... %s\n", myId, QPIdx, GetWRSummary(wr).c_str());

        int retval = ibv_post_send(endpoints[remote_rank].queue_pair, wr, &bad_wr);
        if (retval != 0) {
            printf("[%d] Error posting send wr to endpoint %d failed. wr.op = %d, .next = %p, .key = %d, .dest = %d, %s, Stack = %s\n", myId, remote_rank, wr->opcode, wr->next, PHUB_MAX_KEY & wr->imm_data, (wr->imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK, GetWRSummary(wr).c_str(), GetStacktraceString().c_str());
            if (wr->next != NULL)
            {
                printf("[%d] Cont' .next.next = %p, .next.op = %d\n", myId, wr->next->next, wr->next->opcode);
            }
            perror("Error posting send WR");

            if (bad_wr) {
                std::cerr << "Error posting send WR at WR " << wr << " (first WR in list was " << bad_wr << ")" << std::endl;
                printf("[%d] Badwr @ endpoint %d ... badwr.op = %d, .next = %p, .key = %d, .dest = %d. wr summary = %s\n", myId, remote_rank, bad_wr->opcode, bad_wr->next, PHUB_MAX_KEY & bad_wr->imm_data, (bad_wr->imm_data >> PHUB_MAX_KEY_BITS) & PHUB_IMM_SENDER_MASK, GetWRSummary(bad_wr).c_str());
            }

            exit(1);
        }
    }
    std::string GetEndpointSummary(int i)
    {
        std::stringstream ss;
        ss << "        verbs->endpoints[" << i << "]. remote lid = " << endpoints[i].lid
            << " .mxnet id = " << myId
            << " .QPID = " << endpoints.at(i).Index
            << " .remote idx = " << endpoints.at(i).RemoteMachineIdx
            << " .qpIdx = " << i
            << " .remote qpIdx = " << endpoints.at(i).RemoteQPIdx
            << " .local lid = " << endpoints.at(i).LocalLid
            << " .local qp = " << endpoints[i].queue_pair->qp_num
            << " .remote qp = " << endpoints[i].qp_num
            << " .device = " << endpoints.at(i).DeviceIdx
            << " .Core Associated = " << endpoints.at(i).CoreIdx
            << " .socket = " << endpoints.at(i).SocketIdx
            << " .SCQ,.RCQ = " << endpoints.at(i).CQIdx
            << std::endl;
        return ss.str();
    }


    std::string Summarize(std::string identifier = "", bool showGlobalKnowledge = false, bool verbose = false)
    {
        std::stringstream ss;
        auto myId = ps::Postoffice::Get()->van()->my_node().id;
        ss << "[" << myId << "][" << identifier << "]" << std::endl;
        ss << "    [Sockets, Cores and Interfaces]" << std::endl;
        for (size_t i = 0; i < SocketCount; i++)
        {
            ss << "    Socket " << i << std::endl;
            for (size_t cidx = 0; cidx < Core2SocketIdx.size(); cidx++)
            {
                if (Core2SocketIdx.at(cidx) == i)
                {
                    ss << "        Core " << cidx << std::endl;
                }
            }
            for (size_t dev = 0; dev < Device2SocketIdx.size(); dev++)
            {
                if (Device2SocketIdx.at(dev) == i)
                {
                    ss << "        Device " << dev << ": " << device_names.at(dev) << " @ " << (int)ports.at(dev);
                    if (DirectConnect == true)
                    {
                        ss << " Remote attached: ";
                        //which remote is attached to this device?
                        for (size_t ridx = 0; ridx < Remote2VirtualDeviceIdx.size(); ridx++)
                        {
                            if (Remote2VirtualDeviceIdx.at(ridx) == dev)
                            {
                                ss << ridx << ", ";
                            }
                        }
                    }
                    ss << std::endl;
                }
            }
        }
        ss << std::endl;
        if (verbose)
        {
            ss << "    [QP and CQ Assignment]" << std::endl;
            for (size_t i = 0; i < endpoints.size(); i++)
            {
                //auto myRole = ps::Postoffice::Get()->van()->my_node().role;
                ss << GetEndpointSummary(i);
            }
        }
        ss << "    [Core to CQ association]" << std::endl;
        for (size_t i = 0; i < core2CQIdxs.size(); i++)
        {
            ss << "        Core  " << i << " : ";
            for (auto cqIdx : core2CQIdxs.at(i))
            {
                ss << cqIdx << " ,";
            }
            ss << std::endl;
        }
        if (showGlobalKnowledge)
        {
            ss << "    [Server Key Assignment]" << std::endl;
            for (size_t i = 0; i < serverKeyCounts.size(); i++)
            {
                ss << "    Server[" << i << "].KeyCount = " << serverKeyCounts[i] << " .QPCount = " << serverQPCounts[i] << std::endl;
            }
        }
        ss << std::endl;
        return ss.str();
    }
    /// set up queue pairs for RDMA operations

    //returns if the counter goes to 0, meaning we need to poll a signaled send now.
    inline bool UpdateQPCounter(size_t qpIdx, int dec)
    {
        *QPStats[qpIdx] -= dec;// dec;
        //printf("[%d]QPStats[%d] = %d, deced = %d\n", myId, (int)qpIdx, (int)QPStats[qpIdx], dec);
    //the two last messages need to do this.
        if (*QPStats[qpIdx] <= 0)
        {
            *QPStats[qpIdx] = GetSQDepth();
            return true;
        }
        else
        {
            return false;
        }
    }

    Endpoint& Helper_Worker_GetEndpointFromKey(int key)
    {
        auto qp = workerKey2QPIdx.at(key);
        return endpoints.at(qp);
    }

    bool Helper_Server_IsItMyKey(int key)
    {
	auto qp = workerKey2QPIdx.at(key);
	return workerQP2SrvId.at(qp) == ps::Postoffice::Get()->my_rank();
    }

    Endpoint& Helper_Server_GetEndpointFromKey(int key, int remoteIdx)
    {
        auto qp = workerKey2QPIdx.at(key);
        CHECK(workerQP2SrvId.at(qp) == ps::Postoffice::Get()->my_rank()) << workerQP2SrvId.at(qp) << " v.s. " << ps::Postoffice::Get()->my_rank();
        auto srvQp = workerQP2SrvQPIdx.at(qp);
        return endpoints.at(remoteIdx * psQPCount + srvQp);
    }

    enum CompletionQueueType
    {
        Send,
        Receive
    };
    /// MPIConnection reference for communication during queue pair and memory region setup
    Verbs(int qpNum,
        int coreCount,
        int psliteid,
        std::unordered_map<int, int>& keySizes,
        bool directConnect,
        std::string desired_device_name, //= "mlx4_0",
        const int8_t desired_port = 1)
        : devices(nullptr)
        , num_devices(0)
        , sendCompletionQueues()
        , receiveCompletionQueues()
        , endpoints()
        , workerQPCount(qpNum)
        , DirectConnect(directConnect)
        , myId(psliteid)
    {
        //worker doesn't care about directconnect.
        if (ps::Postoffice::Get()->num_servers() > 1 && DirectConnect == true)
        {
            printf("[%d][Warning] Don't know what it means by sharding + DirectConnect. DirectConnect is turned off.\n", myId);
            DirectConnect = false;
        }
        if (ps::Postoffice::Get()->num_servers() > 1 || (ps::Postoffice::Get()->van()->my_node().role == ps::Node::WORKER))
        {
            //sharding requires to use only 1 interface.
            //DirectConnect(TM) requires worker to use only 1 interface.
            auto desired_device_name_cstr = getenv("PHUB_PREFERRED_INTERFACE");
            if (desired_device_name_cstr == NULL)
            {
                desired_device_name = "mlx4_0";
            }
            else
            {
                desired_device_name.assign(desired_device_name_cstr);
            }
            //regardless, worker please use only 1 interface for now.
            //sharding turns off DirectConnect.
        }
        if (coreCount == 0)
        {
            raise(SIGTRAP);
        }
        CHECK(coreCount > 0) << "no core? = " << coreCount;
        CoreCount = coreCount;
        initialize_device(desired_device_name, desired_port);
        initialize_queue_pairs(keySizes);
        //server side no need to lock
        SmartPostNoLocking = ps::Postoffice::Get()->van()->my_node().role == ps::Node::SERVER;
        auto bitwidthStr = getenv("PHUB_BITWIDTH_SIM");
        if (bitwidthStr != NULL)
        {
            int bitwidth = atoi(bitwidthStr);
            printf("[%d] PHUB_BITWIDTH_SIM=%d\n", myId, bitwidth);

            if (bitwidth != sizeof(float) * 8 && ps::Postoffice::Get()->van()->HasFeature(ps::Van::SupportsBitwidthSIM) == false)
            {
                printf("[%d] selected a van that does not support bitwidth sim but bitwidth sim is toggled\n", myId);
                exit(1);
            }
            VerbsBitwidthReductionRatio = sizeof(float) * 8 / bitwidth;
        }

        //connect_queue_pairs();
    }

    /// call before ending process
    void finalize() {
        finalize_device();
    }

    /// destructor ensures finalize has been called
    ~Verbs() {
        finalize();
    }

    /// Accessor for protection domain
    /// Register a region of memory with Verbs library
    void VerbsSmartPost(int QPIndex, int cqIndex, int messageCnt, ibv_send_wr* wr)
    {
        //post_send uses a spinlock so it's fine to lock for it here.
        //be aware:: pshub servers do not require locking.
        if (SmartPostNoLocking == false)
        {
            pthread_spin_lock(VSLs[QPIndex]);
        }
        wr->wr_id = QPIndex;
        //std::thread::id this_id = std::this_thread::get_id();
        //std::cout<<"["<<myId<<"][0] tid = "<<this_id << " no lock? " << SmartPostNoLocking << std::endl;
        //if true is removed we are pulling more than expected things out.
        if (UpdateQPCounter(QPIndex, messageCnt))
        {
            if (messageCnt == 2)
            {
                CHECK(wr->next != NULL && wr->next->next == NULL);
                wr->send_flags = 0;
                wr->next->send_flags = IBV_SEND_SIGNALED;
            }
            else
            {
                CHECK(wr->next == NULL);
                wr->send_flags = IBV_SEND_SIGNALED;
            }
            //we need to poll then send.
        //printf("[%d] sending out qpindex = %d, polling cqIdx = %d, wr = %s. ep = %s\n", myId, QPIndex, cqIndex, GetWRSummary(wr).c_str(), GetEndpointSummary(QPIndex).c_str());

            post_send(QPIndex, wr);
            //at most 2.
            /*ibv_wc wc[10];
            int cnter = 0;
            while (cnter != messageCnt)
            {
            auto current = poll(10, cqIndex, Verbs::Send, wc);
            if(current > messageCnt)
            {
            printf("[%d] error polled %d out, max possible is %d\n", myId, current, messageCnt);
            }
            cnter += current;
            if(cnter > messageCnt)
            {
            printf("[%d] error cnter is now %d\n", myId, cnter);
            }
            }*/
            /*        if(myId == 8)*/
            ibv_wc wc;
            while (0 == poll(1, cqIndex, Verbs::Send, &wc));
            /*        if(myId == 8) */
        //printf("[%d] send success qp = %s\n", myId, GetEndpointSummary(QPIndex).c_str());
        }
        else
        {
            wr->send_flags = 0;

            if (messageCnt == 2)
            {
                CHECK(wr->next != NULL);
                wr->next->send_flags = 0;
            }
	    else
	    {
		CHECK(wr->next == NULL);
	    }
            //just send
            post_send(QPIndex, wr);
        }
        //std::cout<<"["<<myId<<"][1] tid = "<<this_id << " no lock? " << SmartPostNoLocking << std::endl;

        if (SmartPostNoLocking == false)
        {
            pthread_spin_unlock(VSLs[QPIndex]);
        }
    }

    /// post a receive request for a remote rank
    ibv_mr * register_memory_region(void * base, size_t size, int devId) {

        CHECK(base != NULL);
        CHECK(size != 0);
        ibv_mr * mr;
        mr = ibv_reg_mr(protection_domains.at(devId),
            base, size,
            (IBV_ACCESS_LOCAL_WRITE |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_READ |
                IBV_ACCESS_REMOTE_ATOMIC));
        if (!mr) {
            std::cerr << "Error registring memory region at " << base << " of " << size << " bytes!\n";
            exit(1);
        }

        return mr;
    }

    /// post a receive request for a remote rank


    static inline std::string GetWRSummary(ibv_send_wr* wr)
    {
        std::stringstream ss;
        if (wr == NULL)
        {
            return "";
        }
        else
        {
            ss << "wr->op = " << wr->opcode << ",->key = " << (PHUB_MAX_KEY & wr->imm_data) << ",->next = " << wr->next << " .rdma.addr = " << wr->wr.rdma.remote_addr << " .rkey = " << wr->wr.rdma.rkey;
            for (size_t i = 0; i < wr->num_sge; i++)
            {
                ss << ", wr->sge[" << i << "].length=" << wr->sg_list[i].length << " .lkey=" << wr->sg_list[i].lkey << " .addr=" << wr->sg_list[i].addr;
            }
            ss << std::endl;
            ss << "...additional wr: " << GetWRSummary(wr->next);
        }
        return ss.str();
    }


    /// consume up to max_entries completion queue entries. Returns number
    /// of entries consumed.
    int poll(int max_entries, int QIndex, Verbs::CompletionQueueType type, ibv_wc* wc) {
        CHECK(QIndex < sendCompletionQueues.size() && QIndex >= 0);
        int retval = -1;
        if (type == CompletionQueueType::Send)
            retval = ibv_poll_cq(sendCompletionQueues[QIndex], max_entries, wc);
        else if (type == CompletionQueueType::Receive)
            retval = ibv_poll_cq(receiveCompletionQueues[QIndex], max_entries, wc);
        if (retval < 0) {
            std::cerr << "Failed polling completion queue with status " << retval << "\n";
            exit(1);
        }
        else if (retval > 0) {
            if (wc->status == IBV_WC_SUCCESS) {
                if (wc->opcode == IBV_WC_RDMA_WRITE) {
#ifdef VERBOSE
                    std::cout << "Got completion for WR ID " << wc.wr_id << std::endl;
#endif
                }
                else if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
#ifdef VERBOSE
                    std::cout << "Got completion for WR ID " << wc.wr_id << " with immediate value " << (void*)((int64_t)wc.imm_data) << std::endl;
#endif
                }
                else {
#ifdef VERBOSE
                    std::cout << "Got completion for something with id " << ((int64_t)wc.wr_id) << std::endl;
#endif
                }
            }
            else {
                printf("[%d][%d] polling Qidx=%d IsSendQ=%d got status %s. SendCompletionQueue.size() = %d RecvCompletionQueue.size() = %d,  StackTrace=%s, wc->wr_id = %d\n", ps::Postoffice::Get()->van()->my_node().id, ps::Postoffice::Get()->van()->my_node().role, QIndex, type == CompletionQueueType::Send, ibv_wc_status_str(wc->status), sendCompletionQueues.size(), receiveCompletionQueues.size(), GetStacktraceString().c_str(), wc->wr_id);
		CHECK(false) << " Details to follow: ID = " <<  ps::Postoffice::Get()->van()->my_node().id << " Send=" << (type == CompletionQueueType::Send) << " Idx=" << QIndex << " error= " << strerror(errno);
            }
        }
        return retval;
    }

    void CheckMapIntegrity(int keyCount)
    {
        //check size of endpoints.
        auto myRole = ps::Postoffice::Get()->van()->my_node().role;
        auto numServer = ps::Postoffice::Get()->num_servers();
        auto numWorker = ps::Postoffice::Get()->num_workers();
        auto myRank = ps::Postoffice::Get()->my_rank();
        if (myRole == ps::Node::WORKER)
        {
            CHECK(endpoints.size() == workerQPCount);
        }
        else
        {
            CHECK(endpoints.size() == psQPCount * numWorker);
        }
        //first scan all endpoints, make sure all necessary fields are assigned.

        for (size_t i = 0; i < endpoints.size(); i++)
        {
            auto& ep = endpoints[i];
            CHECK(ep.lid != -1);
            CHECK(ep.queue_pair != NULL);
            CHECK(ep.qp_num != -1);
            CHECK(ep.CoreIdx >= 0 && ep.CoreIdx < CoreCount);
            CHECK(ep.DeviceIdx >= 0 && ep.DeviceIdx < virtual_devices.size());
            CHECK(ep.LocalLid != -1);
            CHECK(ep.RemoteKey != 0);
            CHECK(ep.RemoteMachineIdx != -1);
            CHECK(ep.RemoteQPIdx != -1);
            CHECK(ep.SocketIdx >= 0 && ep.SocketIdx < SocketCount);
            CHECK(ep.CQIdx >= 0 && ep.CQIdx < sendCompletionQueues.size());
            //make sure the maps agree with which socket everything belongs to.
            CHECK(Core2SocketIdx.at(ep.CoreIdx) == ep.SocketIdx) << "ep.CoreIdx = " << ep.CoreIdx << " core2socketidx.at(Ep.coreidx) = " << Core2SocketIdx.at(ep.CoreIdx) << " vs ep.SocketIdx = " << ep.SocketIdx;
            CHECK(Device2SocketIdx.at(ep.DeviceIdx) == ep.SocketIdx);
        }

        //everyone check workerKey2QPIdx map.
        CHECK(workerKey2QPIdx.size() == keyCount);
        //check that there are only qpcount values in workerKey2QPIdx map.
        for (auto qpIdx : workerKey2QPIdx)
        {
            CHECK(qpIdx >= 0 && qpIdx < workerQPCount);
        }

        //check remote keys.
        //check remote keys are assigned.

        //check send and receive completion queues.
        if (virtual_devices.size() == 1)
        {
            //less than because some servers may not be serving keys for very small neural networks.
            CHECK(sendCompletionQueues.size() <= CoreCount);
            CHECK(receiveCompletionQueues.size() <= CoreCount);
        }

        //Check integrity of server/workerQP2CQIdx.
        //duplicate code so when CHECK fails we know which role caused problem.
        //check workerQP2SrvId is consistent.
        CHECK(workerQP2SrvId.size() == workerQPCount);
        for (auto srvId : workerQP2SrvId)
        {
            CHECK(srvId >= 0 && srvId < numServer);
        }

        //check server qp sizes.
        CHECK(serverQPCounts.size() == numServer);
        //check qp to server and qp to server qpidx.
        //just make sure they are within range.
        std::vector<int> _serverQPSizes(numServer);
        for (size_t i = 0; i < workerQPCount; i++)
        {
            auto serverId = workerQP2SrvId[i];
            _serverQPSizes[serverId]++;
        }

        //check _serverQPSizes match with serverQPCounts
        for (size_t i = 0; i < numServer; i++)
        {
            CHECK(_serverQPSizes[i] == serverQPCounts[i]);
        }

        //check that workerQP2SrvQPIdx is within bound of each server.
        for (size_t i = 0; i < workerQPCount; i++)
        {
            auto serverId = workerQP2SrvId[i];
            CHECK(workerQP2SrvQPIdx[i] >= 0 && workerQP2SrvQPIdx[i] < serverQPCounts[serverId]);
        }

        CHECK(serverIDQPIdx2WkrQPIdx.size() == numServer);
        //check serverIDQPIdx2WkrQPIdx is consistent
        for (size_t i = 0; i < workerQPCount; i++)
        {
            auto serverId = workerQP2SrvId[i];
            auto serverQPIdx = workerQP2SrvQPIdx[i];
            CHECK(serverIDQPIdx2WkrQPIdx[serverId].size() > serverQPIdx);
            CHECK(serverIDQPIdx2WkrQPIdx[serverId][serverQPIdx] == i);
        }

        //Check serverKeyCounts is correct.
        CHECK(serverKeyCounts.size() == numServer);
        size_t totalKeys = 0;
        for (auto kCnt : serverKeyCounts)
        {
            CHECK(kCnt >= 0);
            totalKeys += kCnt;
        }
        CHECK(totalKeys == keyCount);

        //make sure Cores cover all CQs.
        size_t totalCQs = sendCompletionQueues.size();
        std::unordered_set<int> cqs;
        size_t covered = 0;
        for (size_t i = 0; i < core2CQIdxs.size(); i++)
        {
            covered += core2CQIdxs.at(i).size();
            for (auto cq : core2CQIdxs.at(i))
            {
                cqs.insert(cq);
            }
        }
        //CHECK all covered.
        CHECK(cqs.size() == totalCQs);
        //CHECK uniquely covered.
        CHECK(covered == totalCQs);

        //CHECK a key is associated with one socket, or number of devices
        //on the server side.
        if (myRole == Node::SERVER)
        {
            if (DirectConnect == false)
            {
                for (size_t j = 0; j < keyCount; j++)
                {
                    std::unordered_set<int> keyDevice;
                    std::unordered_set<int> keySockets;
                    auto wQidx = workerKey2QPIdx.at(j);
                    auto srvIdx = workerQP2SrvId.at(wQidx);
                    if (srvIdx == myRank)
                    {
                        auto srvQpidx = workerQP2SrvQPIdx.at(wQidx);
                        for (size_t i = 0; i < numWorker; i++)
                        {
                            auto& ep = Helper_Server_GetEndpointFromKey(j, i);
                            CHECK(srvQpidx + i * psQPCount == ep.Index);
                            keyDevice.insert(ep.DeviceIdx);
                            keySockets.insert(ep.SocketIdx);
                            CHECK(Core2SocketIdx.at(ep.CoreIdx) == ep.SocketIdx);
                        }
                        //a switch directs same key from all workers to the same device and socket.
                        CHECK(keyDevice.size() == 1);
                        CHECK(keySockets.size() == 1);
                    }
                }
            }
            else
            {
                //a directconnect configuration.
                //check that keys from each same worker maps to the same interface.
                for (int i = 0; i < numWorker; i++)
                {
                    std::unordered_set<int> keyDevice;
                    std::unordered_set<int> keySockets;
                    CHECK(numServer == 1) << " Sharding and DirectConnect is incompatible.";
                    for (int j = 0; j < keyCount; j++)
                    {
                        auto& endpoint = Helper_Server_GetEndpointFromKey(j, i);
                        keyDevice.insert(endpoint.DeviceIdx);
                        keySockets.insert(endpoint.SocketIdx);
                    }
                    //CHECK that keydevice and keysockets are 1.
                    CHECK(keyDevice.size() == 1);
                    CHECK(keySockets.size() == 1);
                }
            }
        }
    }
    inline void post_receive(int QPIdx, ibv_recv_wr * wr) {
        ibv_recv_wr * bad_wr = nullptr;
        int retval = ibv_post_recv(endpoints[QPIdx].queue_pair, wr, &bad_wr);
        if (retval != 0) {
            //        char buf[500];
            //print_stacktrace();
            printf("[%d]Error posting receive WR to endpoint %d. errno = %d. More informatin follows. Stack = %s\n", myId, QPIdx, retval, GetStacktraceString().c_str());
            perror("Error posting receive");
            exit(1);
        }
        if (bad_wr) {
            printf("[%d]Error posting receive WR. BadWR = %p, CurrentWR= %p\n", ps::Postoffice::Get()->van()->my_node().id, bad_wr, wr);
            exit(1);

        }

        //printf("[%d][success] attempting to post receive wr to endpoint %d\n", myId, QPIdx);
    }
};
