/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_INTERNAL_VAN_H_
#define PS_INTERNAL_VAN_H_
#include <unordered_map>
#include <mutex>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <ctime>
#include "dmlc/DIME.h"
#include "ps/base.h"
#include "ps/internal/message.h"
#include <unordered_map>
#include <unordered_set>
namespace ps {
    class Resender;
    /**
     * \brief Van sends messages to remote nodes
     *
     * If environment variable PS_RESEND is set to be 1, then van will resend a
     * message if it no ACK messsage is received within PS_RESEND_TIMEOUT millisecond
     */
    class Van {
    public:
        void virtual InitializeVanOptimizerValues(uint num, float* values)
        {

        }

        enum VanFeatureSet
        {
            NativeInfiniband = 1,
            RequiresSendBufferKeyCopyAsServer = 2,
            OnlyDeliversMergedBuffer = 4,
            WorkerSidePushPullZeroCopy = 8,
            SupportsKeyChunking = 16,
            SupportsMultiServer = 32,
            VerbsPostSendNoLocking = 64,
            SupportsBitwidthSIM = 128,
            PullRequestElision = 256,
            MetadataElision = 512
        };
        /**
         * \brief create Van
         * \param type zmq, socket, ...
         */
        static Van* Create(const std::string& type);
        /** \brief constructer, do nothing. use \ref Start for real start */
        Van() { }
        /**\brief deconstructer, do nothing. use \ref Stop for real stop */
        virtual ~Van() { }
        /**
         * \brief start van
         *
         * must call it before calling Send
         *
         * it initalizes all connections to other nodes.  start the receiving
         * threads, which keeps receiving messages. if it is a system
         * control message, give it to postoffice::manager, otherwise, give it to the
         * accoding app.
         */
        virtual void Start();
	virtual void WaitForQPExchanges(int count)
	{
	}
        //This is only used if Chunking is enabled.
        virtual int PHUBGetKeySharding(Key key)
        {
            return 0;
        }

        virtual bool FullyInitialized()
        {
            return true;
        }

        //cpuid
        inline bool HasFeature(uint64_t featureFlag)
        {
            return (featureFlag & FeatureSet) == featureFlag;
        }

        /**
         * \brief send a message, It is thread-safe
         * \return the number of bytes sent. -1 if failed
         */
        int Send(const Message& msg);
        /**
         * \brief return my node
         */
        const Node& my_node() const {
            if (!ready_)
            {
                print_stacktrace();
            }
            CHECK(ready_) << "call Start() first";
            return my_node_;
        }
        /**
         * \brief stop van
         * stop receiving threads
         */
        virtual void Stop();
        /**
         * \brief get next available timestamp. thread safe
         */
        int GetTimestamp() { return timestamp_++; }
        /**
         * \brief whether it is ready for sending. thread safe
         */
        bool IsReady() { return ready_; }

        //Sets the key size and key address (for workers).
        //Server: ignore key address.
        void SetKeySize(int key, int bytes, uint64_t kAddr = 0, uint64_t kRecvAddr = 0)
        {
            //printf("[%d]Key = [%d], Size = [%d]\n", my_node_.id, key, bytes);
            keySize[key] = bytes;
            if (kAddr != 0)
                vKeyAddress[key] = kAddr;
            if (kRecvAddr != 0)
                vKeyRecvAddress[key] = kRecvAddr;
        }

        virtual void HandleSpecialCommand(Message& msg)
        {

        }

        virtual void OnKeyPopulated()
        {

        }

        virtual std::string VanType()
        {
            return "AbstractVan";
        }

    protected:
        /**
         * \brief connect to a node
         */
        virtual void Connect(const Node& node) = 0;
        /**
         * \brief bind to my node
         * do multiple retries on binding the port. since it's possible that
         * different nodes on the same machine picked the same port
         * \return return the port binded, -1 if failed.
         */
        virtual int Bind(const Node& node, int max_retry) = 0;
        /**
         * \brief block until received a message
         * \return the number of bytes received. -1 if failed or timeout
         */
        virtual int RecvMsg(Message* msg) = 0;
        /**
         * \brief send a mesage
         * \return the number of bytes sent
         */
        virtual int SendMsg(const Message& msg) = 0;

        //void PackMeta(const Meta& meta, char* existingBuf, int* buf_size);

        /**
         * \brief pack meta into a string
         */
        void PackMeta(const Meta& meta, char** meta_buf, int* buf_size);
        /**
         * \brief unpack meta from a string
         */
        void UnpackMeta(const char* meta_buf, int buf_size, Meta* meta);



        Node scheduler_;
        Node my_node_;
        bool is_scheduler_;
        /*Size for each key*/
        std::unordered_map<int, int> keySize;
        std::unordered_map<int, uint64_t> vKeyRecvAddress;
        //address for virtual key. usually they are part of keyAddress, which is physical
        std::unordered_map<int, uint64_t> vKeyAddress;
    private:

        /** thread function for receving */
        void Receiving();
        /** thread function for heartbeat */
        void Heartbeat();
        /** whether it is ready for sending */
        std::atomic<bool> ready_{ false };
        std::atomic<size_t> send_bytes_{ 0 };
        size_t recv_bytes_ = 0;
        int num_servers_ = 0;
        int num_workers_ = 0;
        /** the thread for receiving messages */
        std::unique_ptr<std::thread> receiver_thread_;
        /** the thread for sending heartbeat */
        std::unique_ptr<std::thread> heartbeat_thread_;
        std::vector<int> barrier_count_;
        std::unordered_set<int> dbgMsg[8];
        /** msg resender */
        Resender* resender_ = nullptr;
        int drop_rate_ = 0;
        std::atomic<int> timestamp_{ 0 };
        DISALLOW_COPY_AND_ASSIGN(Van);
    protected:
        uint64_t FeatureSet;

    };
}  // namespace ps
#endif  // PS_INTERNAL_VAN_H_
