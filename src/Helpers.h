#pragma once
#include <thread>
#include <numa.h>
#include <malloc.h>
#include <numaif.h>
#include <stdio.h>      /* printf */
#include <math.h>       /* ceil */
#include <vector>

static int GetCurrentThreadNumaNode()
{
    int cpu = sched_getcpu();
    return numa_node_of_cpu(cpu);
}
//this name does not reflect the actual action of the function.
static void MigrateToNumaNode(int nodeId)
{
    //1. MIGRATE
    //2. SET POLICY
    //3. DONE.
    //Make sure there is no parallel calls to this, as conflicting policies may be set.
    //bitmask* bm= numa_allocate_nodemask(); 
    //numa_bitmask_setbit(bm, nodeId);
    //numa_set_membind(bm);
    //numa_free_nodemask(bm);
    //return;
    if(nodeId == -1) return;
    CHECK(nodeId <= 1 && nodeId >= 0);
    //numa_set_preferred(nodeId);
    //use static ids.
    unsigned long allowedSock =  1<<nodeId;
    CHECK(set_mempolicy(MPOL_BIND, &allowedSock, 3) == 0) << " Failed set_mempolicy to "<< nodeId << " "<<strerror(errno);
    /*if (nodeId == GetCurrentThreadNumaNode() || numa_available() < 0) return;
    
    CHECK(nodeId >= 0 && nodeId < 2) << "requested nodeId = " << nodeId << " is not accepted. hardcoded: numa configured = " << 2;
    //we're assuming each numa node has exactly the same processor count,
    //and assuming processor ids are continous in a numa domain
    //and assuming there is no HT.
    auto totalProc = std::thread::hardware_concurrency();
    auto perSock = 1.0 * totalProc / 2;
    CHECK(perSock == (int)perSock) << "Unbalanced numa configurations? If numa_available returns 0 we default to 2 Numa Nodes. ";
    auto targetProc = perSock * nodeId;
    //now migrate myself.
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(targetProc, &cpuset);
    CHECK(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0) << "Failed to set affinity for thread to numa node " << nodeId << ". Proc Target = " << targetProc;
    CHECK(GetCurrentThreadNumaNode() == nodeId) << "Migration Failed?";*/
    //numa_set_preferred(-1);
}


static int GetAddressNumaNode(void* ptr)
{
    //default.
    if(numa_available() == -1) return 0;
    int numa_node = -1;
    get_mempolicy(&numa_node, NULL, 0, (void*)ptr, MPOL_F_NODE | MPOL_F_ADDR);
    return numa_node;
}

static void* AlignedAllocateUniversal(size_t bytes, int sock = -1, size_t alignment = 2097152)
{
    CHECK(sock == -1 || (sock >= 0 && sock <=1)) << " Currently checks only assumes up to 2 Numa Node. ";
    if(sock != -1)
    {
	//make sure numa_available is ok.
	CHECK(numa_available() != -1);
    }
    MigrateToNumaNode(sock);
    //cant tell if successful.
    //use memalign.
    //auto ret = numa_alloc_onnode(bytes, sock);//
    auto ret = memalign(alignment, bytes);
    memset(ret, 0, bytes);

    CHECK(ret) << "Allocation failed.";
    //if no numa is available, or no sock preference, do not attempt anything.
    if(sock == -1)
    {
	return ret;
    }
    //attempt to move pages.
    const int PageSize = 4096;
    const int PageSizeMask = PageSize - 1;
    auto ret64 = (uint64_t)ret;
    size_t adjustedBytes = (ret64 % PageSize) + bytes;
    size_t pageCnt = (int)ceil(1.0 * adjustedBytes / PageSize);

    //create params for numa_move_pages.
    std::vector<void*> pages;

    void* base = (void*)((ret64 / PageSize) * PageSize);
    for(int i = 0; i < pageCnt; i++)
    {
	pages.push_back(base + i * PageSize);
    }
    std::vector<int> destinationNodes(pageCnt, sock);
    std::vector<int> status(pageCnt);

    auto retVal = numa_move_pages(getpid(), pages.size(), pages.data(), destinationNodes.data(), status.data(), MPOL_MF_MOVE);
    CHECK(retVal == 0) << "numa_move_pages failed";
    //nowm move
    //move these pages.
    numa_set_preferred(-1);
    //verify on selected socket.
    if(sock != -1)
    {
	for(int i = 0; i < pageCnt; i++)
	{
	    auto test = pages.at(i);
	    int numa_node = GetAddressNumaNode(test);
	    //make sure the returned numa_node is sock
	    //std::cout << "Did not allocate on requested location? actual node = " << numa_node << ". Requested node = " << sock << " Page: " << i << " @ " << test << std::endl;
	}
    }
    return ret;
}

