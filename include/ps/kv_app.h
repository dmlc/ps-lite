/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_KV_APP_H_
#define PS_KV_APP_H_
#include <algorithm>
#include <utility>
#include <vector>
#include "ps/base.h"
#include "ps/simple_app.h"
#include "ps/ps.h"
namespace ps {

    /**
     * \brief the structure for a list of key-value pairs
     *
     * The keys must be unique and sorted in an increasing order.  The length of a
     * value can be more than one. If \a lens is empty, then the length
     * of a value is determined by `k=vals.size()/keys.size()`.  The \a i-th KV pair
     * is then
     *
     * \verbatim {keys[i], (vals[i*k], ..., vals[(i+1)*k-1])} \endverbatim
     *
     * If \a lens is given, then `lens[i]` is the length of the \a i-th
     * value. Let
     *
     * \verbatim n = lens[0] + .. + lens[i-1]  \endverbatim
     *
     * then the \a i-th KV pair is presented as
     *
     * \verbatim {keys[i], (vals[n], ..., vals[lens[i]+n-1])} \endverbatim
     */
    template <typename Val>
    struct KVPairs {
        // /** \brief empty constructor */
        // KVPairs() {}
        /** \brief the list of keys */
        SArray<Key> keys;
        /** \brief the according values */
        SArray<Val> vals;
        /** \brief the according value lengths (could be empty) */
        SArray<int> lens;
    };

    /**
     * \brief A worker node that can \ref Push (\ref Pull) key-value pairs to (from) server
     * nodes
     *
     * \tparam Val the type of value, which should be primitive types such as
     * int32_t and float
     */
    template<typename Val>
    class KVWorker : public SimpleApp {
    public:
        /** avoid too many this-> */
        using SimpleApp::obj_;
        /**
         * \brief callback function for \ref Push and \ref Pull
         *
         * It is called by the data receiving thread of this instance when the push or
         * pull is actually finished. Namely the kv pairs have already written into
         * servers' data structure or the kv pairs have already pulled back.
         */
        using Callback = std::function<void()>;

        std::vector<Callback> PHUBDeterministicCallbacks;
        std::vector<int>      PHUBDeterministicChunkCounters;
        std::vector<int>      PHUBVirtualToPhysicalKeyMapping;

        /**
         * \brief constructor
         *
         * \param app_id the app id, should match with \ref KVServer's id
         */
        explicit KVWorker(int app_id) : SimpleApp() {
            using namespace std::placeholders;
            slicer_ = std::bind(&KVWorker<Val>::DefaultSlicer, this, _1, _2, _3);
            obj_ = new Customer(app_id, std::bind(&KVWorker<Val>::Process, this, _1));
        }

        /** \brief deconstructor */
        virtual ~KVWorker() { delete obj_; obj_ = nullptr; }

        /**
         * \brief Pushes a list of key-value pairs to all server nodes.
         *
         * This function pushes a KV list specified by \a keys and \a vals to all
         * server nodes.
         *
         * Sample usage: the following codes push two KV pairs `{1, (1.1, 1.2)}` and `{3,
         * (3.1,3.2)}` to server nodes, where the value is a length-2 float vector
         * \code
         *   KVWorker<float> w;
         *   std::vector<Key> keys = {1, 3};
         *   std::vector<float> vals = {1.1, 1.2, 3.1, 3.2};
         *   w.Push(keys, vals);
         * \endcode
         *
         * If \a lens is given, then the value can be various length. See
         * \ref KVPairs for more information.
         *
         * The KV list is partitioned and sent based on the key range each server
         * maintaining. This function returns without waiting the data are sent
         * actually. Instead, use either \ref Wait or the callback to know when
         * finished. This function is thread-safe.
         *
         * @param keys a list of keys, must be unique and sorted in increasing order
         * @param vals the according values
         * @param lens optional, lens[i] stores the value length of the \a
         * i-th KV pair
         * @param cmd an optional command sent to the servers
         * @param cb the callback which is called when the push is finished.
         * @return the timestamp of this request
         */
        int Push(const std::vector<Key>& keys,
            const std::vector<Val>& vals,
            const std::vector<int>& lens = {},
            int cmd = 0,
            const Callback& cb = nullptr) {
            return ZPush(
                SArray<Key>(keys), SArray<Val>(vals), SArray<int>(lens), cmd, cb);
        }

        /**
         * \brief Pulls the values associated with the keys from the server nodes
         *
         * This function pulls the values of the keys specified in \a keys from the
         * server nodes. The format is same to \ref KVPairs
         *
         * Sample usage: the following codes pull the values of keys \a 1 and \a 3
         * from the server nodes.
         * \code
         *   KVWorker<float> w;
         *   std::vector<Key> keys = {1, 3};
         *   std::vector<float> vals;
         *   ps.Pull(keys, &vals);
         * \endcode
         *
         * It's a non-blocking call. The actual pulling is finished,
         * namely \a vals (and \a lens) is filled with pulled values, only
         * if \ref Wait returns or the callback is called.
         *
         * @param keys a list of keys, must be unique and sorted in increasing order
         * @param vals the buffer for the pulled values. It can be 0 size.
         * @param lens optional buffer for the value length. If set, it can be 0 size.
         * @param cmd an optional command sent to the servers
         * @param cb the callback which is called when the pull is finished.
         * @return the timestamp of this request
         */
        int Pull(const std::vector<Key>& keys,
            std::vector<Val>* vals,
            std::vector<int>* lens = nullptr,
            int cmd = 0,
            const Callback& cb = nullptr) {
            return Pull_(SArray<Key>(keys), vals, lens, cmd, cb);
        }

        /**
         * \brief Waits until a push or pull has been finished
         *
         * Sample usage:
         * \code
         *   int ts = w.Pull(keys, &vals);
         *   Wait(ts);
         *   // now vals is ready for use
         * \endcode
         *
         * \param timestamp the timestamp returned by the push or pull
         */
        void Wait(int timestamp)
        {
            //if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false)
            //{
                obj_->WaitRequest(timestamp);
		//}
		//else
		//{
                //this isn't really called many times...
                //while (0 != PHUBDeterministicChunkCounters.at(timestamp))
                //{
                //    usleep(1000);
                //}
		//}
        }

        /**
         * \brief zero-copy Push
         *
         * This function is similar to \ref Push except that all data
         * will not be copied into system for better performance. It is the caller's
         * responsibility to keep the content to be not changed before actually
         * finished.
         */
        int ZPush(const SArray<Key>& keys,
            const SArray<Val>& vals,
            const SArray<int>& lens = {},
            int cmd = 0,
            const Callback& cb = nullptr) {
            //this needs to be changed to other to accomodate infiniband

            int ts = -1;
            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::SupportsKeyChunking) == false)
            {
                ts = obj_->NewRequest(kServerGroup);
            }
            else if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false || keys[0] == -1)
            {
                //potentially a zmq van with 1 server or a infinband van.
                //or initialization of phub
                CHECK(keys.size() > 0);
                auto cnt = keys[0] == -1 ? ps::Postoffice::Get()->num_servers() : keys.size();
                ts = obj_->NewRequestKeyChunk(cnt);

            }
            else
            {
		//does nothing. the ts and etc is grabbed later
                //a pshub van. it gets timestamp from key, not new request, but first, if it's a key size broadcast,
                //CHECK(keys.size() == 1) << " sending " << keys.size() << " keys?";
                //take this opportunity to check if these are properly checked.
                //CHECK(PHUBDeterministicCallbacks.size() > 0);
                //CHECK(PHUBDeterministicChunkCounters.size() > 0);
                //CHECK(PHUBVirtualToPhysicalKeyMapping.size() > 0);
                //grab a ts.
                //auto cnt = keys.size();
		//these keys belong to the same virtual key.
                ts = PHUBVirtualToPhysicalKeyMapping.at(keys[0]);
		//CHECK(ts < PHUBDeterministicChunkCounters.size());
		//	//printf("phsyical key = %d's counter is now %d\n", ts, PHUBDeterministicChunkCounters[ts]);
            }
            //printf("ZPushing Keys... %d\n", keys[0]);
            CHECK(ts != -1);
            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false || keys[0] == -1)
            {
                AddCallback(ts, cb);
            }
            else
            {
                PHUBAddCallbackWithDeterministicKey(ts, cb);
            }
            KVPairs<Val> kvs;
            kvs.keys = keys;
            kvs.vals = vals;
            kvs.lens = lens;
            Send(ts, true, cmd, kvs);
            return ts;
        }

        /**
         * \brief zero-copy Pull
         *
         * This function is similar to \ref Pull except that all data
         * will not be copied into system for better performance. It is the caller's
         * responsibility to keep the content to be not changed before actually
         * finished.
         */
        int ZPull(const SArray<Key>& keys,
            SArray<Val>* vals,
            SArray<int>* lens = nullptr,
            int cmd = 0,
            const Callback& cb = nullptr) {
            return Pull_(keys, vals, lens, cmd, cb);
        }
        using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;
        /**
         * \brief a slicer partitions a key-value list according to the key ranges
         * \param send the kv list for partitioning
         * \param ranges the key ranges, ranges[i] is the key range of server i
         * \param sliced the sliced lists. slices[i] should only contains keys in
         * ranges[i] and the according values
         */
        using Slicer = std::function<void(
            const KVPairs<Val>& send, const std::vector<Range>& ranges,
            SlicedKVs* sliced)>;

        /**
         * \brief set a user-defined slicer
         */
        void set_slicer(const Slicer& slicer) {
            CHECK(slicer); slicer_ = slicer;
        }

    private:
        /**
         * \brief internal pull, C/D can be either SArray or std::vector
         */
        template <typename C, typename D>
        int Pull_(const SArray<Key>& keys, C* vals, D* lens,
            int cmd, const Callback& cb);
        /**
         * \brief add a callback for a request. threadsafe.
         * @param cb callback
         * @param timestamp the timestamp of the request
         */
        void AddCallback(int timestamp, const Callback& cb) {
            if (!cb) return;
            //non phub vans/
            std::lock_guard<std::mutex> lk(mu_);
            callbacks_[timestamp] = cb;

        }

        void PHUBAddCallbackWithDeterministicKey(int key, const Callback& cb)
        {
            CHECK(cb);
            CHECK(PHUBDeterministicCallbacks.at(key) == nullptr) << "["<<ps::Postoffice::Get()->van()->my_node().id<<"] key "<<key<<" has already registered an uncalled callback.";
            PHUBDeterministicCallbacks[key] = cb;
        }

        void PHUBRunCallbackWithDeterministicKey(int key)
        {
            auto cb = PHUBDeterministicCallbacks.at(key);
            CHECK(cb);
	    PHUBDeterministicCallbacks.at(key) = nullptr;
            cb();
        }

        /**
         * \brief run and delete the callback
         * \param timestamp the timestamp of the callback
         */
        void RunCallback(int timestamp);
        /**
         * \brief send the kv list to all servers
         * @param timestamp the timestamp of the request
         * @param push whether or not it is a push request
         * @param cmd command
         */
        void Send(int timestamp, bool push, int cmd, const KVPairs<Val>& kvs);
        /** \brief internal receive handle */
        void Process(const Message& msg);
        /** \brief default kv slicer */
        void DefaultSlicer(const KVPairs<Val>& send,
            const std::vector<Range>& ranges,
            SlicedKVs* sliced);

        /** \brief data buffer for received kvs for each timestamp */
        std::unordered_map<int, std::vector<KVPairs<Val>>> recv_kvs_;
        /** \brief callbacks for each timestamp */
        std::unordered_map<int, Callback> callbacks_;
        /** \brief lock */
        std::mutex mu_;
        /** \brief kv list slicer */
        Slicer slicer_;
    };

    /** \brief meta information about a kv request */
    struct KVMeta {
        /** \brief the int cmd */
        int cmd;
        /** \brief whether or not this is a push request */
        bool push;
        /** \brief sender's node id */
        int sender;
        /** \brief the associated timestamp */
        int timestamp;
        /*For other purposes*/
        uint64_t additionalPayload;
        //flexible payload
        uint64_t additionalPayload2;
    };

    /**
     * \brief A server node for maintaining key-value pairs
     */
    template <typename Val>
    class KVServer : public SimpleApp {
    public:
        /**
         * \brief constructor
         * \param app_id the app id, should match with \ref KVWorker's id
         */
        explicit KVServer(int app_id) : SimpleApp() {
            //printf("KVSERVER.h 294\n");

            using namespace std::placeholders;
            //printf("KVSERVER.h 295\n");

            obj_ = new Customer(app_id, std::bind(&KVServer<Val>::Process, this, _1));
            //printf("KVSERVER.h 296\n");
        }

        /** \brief deconstructor */
        virtual ~KVServer() { delete obj_; obj_ = nullptr; }

        /**
         * \brief the handle to process a push/pull request from a worker
         * \param req_meta meta-info of this request
         * \param req_data kv pairs of this request
         * \param server this pointer
         */
        using ReqHandle = std::function<void(const KVMeta& req_meta,
            const KVPairs<Val>& req_data,
            KVServer* server)>;
        void set_request_handle(const ReqHandle& request_handle) {
            CHECK(request_handle) << "invalid request handle";
            request_handle_ = request_handle;
        }

        /**
         * \brief response to the push/pull request
         * \param req the meta-info of the request
         * \param res the kv pairs that will send back to the worker
         */
        void Response(const KVMeta& req, const KVPairs<Val>& res = KVPairs<Val>());

    private:
        /** \brief internal receive handle */
        void Process(const Message& msg);
        /** \brief request handle */
        ReqHandle request_handle_;
    };


    /**
     * \brief an example handle adding pushed kv into store
     */
    template <typename Val>
    struct KVServerDefaultHandle {
        void operator()(
            const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::OnlyDeliversMergedBuffer) == false)
            {
                //INFINIBAND, ZMQ and etc.
                size_t n = req_data.keys.size();
                KVPairs<Val> res;
                if (req_meta.push) {
                    CHECK_EQ(n, req_data.vals.size());
                }
                else {
                    res.keys = req_data.keys; res.vals.resize(n);
                }
                for (size_t i = 0; i < n; ++i) {
                    Key key = req_data.keys[i];
                    if (req_meta.push) {
                        store[key] += req_data.vals[i];
                    }
                    else {
                        res.vals[i] = store[key];
                    }
                }
                server->Response(req_meta, res);
            }
            else
            {
                //this is a pshub van.
                //it must have collected everything, so i do nothing.
                //copy to destination.
                CHECK(req_data.keys.size() == 1) << " key size is " << req_data.keys.size();//PSHUB requires a single key.
                      //PSHUB tests use a single value for each key.
                KVPairs<Val> res;
                auto asVectorPtr = (std::vector<std::pair<int, int>>*)req_meta.additionalPayload2;

                if (req_meta.push)
                {
                    //just store
                    auto sz = asVectorPtr->size();
                    CHECK(sz == ps::Postoffice::Get()->num_workers()) << asVectorPtr->size() << " vs " << ps::Postoffice::Get()->num_workers();
                    store[req_data.keys[0]] += req_data.vals[0];
                    CHECK(req_meta.additionalPayload2 != NULL);
                    std::vector<std::pair<int, int>> cpy(*asVectorPtr);
                    asVectorPtr->clear();
                    for (auto pair : cpy)
                    {
                        ps::KVMeta meta;
                        meta.additionalPayload = req_meta.additionalPayload;
                        //meta.additionalPayload = key;
                        meta.additionalPayload2 = 0;
                        meta.cmd = req_meta.cmd;
                        meta.push = req_meta.push;
                        meta.timestamp = pair.second;
                        meta.sender = pair.first;
                        server->Response(meta);
                    }
                }
                else
                {
                    res.keys = req_data.keys;
                    res.vals.push_back(store[req_data.keys[0]]);
                    server->Response(req_meta, res);
                }
                //send as many responses as there are components in the package
            }
        }
        std::unordered_map<Key, Val> store;
    };


    ///////////////////////////////////////////////////////////////////////////////

    template <typename Val>
    void KVServer<Val>::Process(const Message& msg) {
        if (msg.meta.simple_app) {
            SimpleApp::Process(msg); return;
        }
        KVMeta meta;
        meta.cmd = msg.meta.head;
        meta.push = msg.meta.push;
        meta.sender = msg.meta.sender;
        meta.timestamp = msg.meta.timestamp;
        KVPairs<Val> data;
        int n = msg.data.size();
        if (n) {
            CHECK_GE(n, 2);
            data.keys = msg.data[0];
            CHECK(data.keys.size() == 1) << "Received unknown msg = " << msg.DebugString();
            //{
                //printf("key size is 1 and additional payload is set to %d\n", data.keys[0]);
            meta.additionalPayload = data.keys[0];
            //}
                //this is a pointer cpy
            meta.additionalPayload2 = msg.meta.control.msg_sig;
            data.vals = msg.data[1];
            if (n > 2) {
                CHECK_EQ(n, 3);
                data.lens = msg.data[2];
                CHECK_EQ(data.lens.size(), data.keys.size());
            }
        }
        CHECK(request_handle_);
        request_handle_(meta, data, this);
    }

    template <typename Val>
    void KVServer<Val>::Response(const KVMeta& req, const KVPairs<Val>& res) {
        Message msg;
        msg.meta.customer_id = obj_->id();
        msg.meta.request = false;
        msg.meta.push = req.push;
        msg.meta.head = req.cmd;
        msg.meta.timestamp = req.timestamp;
        msg.meta.recver = req.sender;
        //TODO:: hack ps-lite to retain key information for Acks
    //This forces us to turn off resender, which is not useful with TCP.
        msg.meta.control.msg_sig = req.additionalPayload;
        if (res.keys.size()) {
            msg.AddData(res.keys);
            msg.AddData(res.vals);
            if (res.lens.size()) {
                msg.AddData(res.lens);
            }
        }
        Postoffice::Get()->van()->Send(msg);
    }

    template <typename Val>
    void KVWorker<Val>::DefaultSlicer(
        const KVPairs<Val>& send, const std::vector<Range>& ranges,
        typename KVWorker<Val>::SlicedKVs* sliced) {
        sliced->resize(ranges.size());

        // find the positions in msg.key
        size_t n = ranges.size();
        std::vector<size_t> pos(n + 1);
        const Key* begin = send.keys.begin();
        const Key* end = send.keys.end();
        for (size_t i = 0; i < n; ++i) {
            if (i == 0) {
                pos[0] = std::lower_bound(begin, end, ranges[0].begin()) - begin;
                begin += pos[0];
            }
            else {
                CHECK_EQ(ranges[i - 1].end(), ranges[i].begin());
            }
            size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
            begin += len;
            pos[i + 1] = pos[i] + len;

            // don't send it to severs for empty kv
            sliced->at(i).first = (len != 0);
        }
        CHECK_EQ(pos[n], send.keys.size());
        if (send.keys.empty()) return;

        // the length of value
        size_t k = 0, val_begin = 0, val_end = 0;
        if (send.lens.empty()) {
            k = send.vals.size() / send.keys.size();
            CHECK_EQ(k * send.keys.size(), send.vals.size());
        }
        else {
            CHECK_EQ(send.keys.size(), send.lens.size());
        }

        // slice
        for (size_t i = 0; i < n; ++i) {
            if (pos[i + 1] == pos[i]) {
                sliced->at(i).first = false;
                continue;
            }
            sliced->at(i).first = true;
            auto& kv = sliced->at(i).second;
            kv.keys = send.keys.segment(pos[i], pos[i + 1]);
            if (send.lens.size()) {
                kv.lens = send.lens.segment(pos[i], pos[i + 1]);
                for (int l : kv.lens) val_end += l;
                kv.vals = send.vals.segment(val_begin, val_end);
                val_begin = val_end;
            }
            else {
                kv.vals = send.vals.segment(pos[i] * k, pos[i + 1] * k);
            }
        }
    }

    static void PopulateMessage(Message* msg, int cid, bool req, bool push, int cmd, int ts, int recvr)
    {
        msg->meta.customer_id = cid;
        msg->meta.request = req;
        msg->meta.push = push;
        msg->meta.head = cmd;
        msg->meta.timestamp = ts;
        msg->meta.recver = recvr;
    }

    template <typename Val>
    void KVWorker<Val>::Send(int timestamp, bool push, int cmd, const KVPairs<Val>& kvs) {
        //regardless of feature test, intercept keys with 1 size and value -1.
        if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::SupportsKeyChunking))
        {
            if (kvs.keys.size() == 1 && kvs.keys[0] == -1)
            {
                Message msg;
                PopulateMessage(&msg, obj_->id(), true, push, cmd, timestamp, ps::Postoffice::Get()->ServerRankToID(0));
                if (kvs.keys.size()) {
                    msg.AddData(kvs.keys);
                    msg.AddData(kvs.vals);
                    if (kvs.lens.size()) {
                        msg.AddData(kvs.lens);
                    }
                }

                //this is a special command
                //printf("Delivering Key Size information to server\n");
                msg.meta.control.cmd = Control::INFINIBANDKEYSIZEEXCHG;
                //-1 key is a broadcast message to all servers.
                for (auto id : ps::Postoffice::Get()->GetNodeIDs(ps::kServerGroup))
                {
                    msg.meta.recver = id;
                    Postoffice::Get()->van()->Send(msg);
                }
                return;
            }
            //sharded ZMQ has SupportsKeyChunking turned off, but can still chunk via other mechanisms (MXNET_KVSTORE_BIGARRAY_BOUND).
            //kvs.vals is a shared buffer.
            if (kvs.keys.size() > 0)
            {
                int cumulativeSizeInFloat = 0;
		if(ps::Postoffice::Get()->van()->HasFeature(Van::MetadataElision))
		{
		    CHECK(PHUBDeterministicChunkCounters.at(timestamp) == 0) << " key[0] = " << kvs.keys[0] << " cnter = " << PHUBDeterministicChunkCounters.at(timestamp);
		    PHUBDeterministicChunkCounters[timestamp] += kvs.keys.size();
		    //printf("key = %d, size incre = %d, counetr = %d\n", kvs.keys[0], kvs.keys.size(), PHUBDeterministicChunkCounters.at(timestamp));
		}

                for (size_t i = 0; i < kvs.keys.size(); i++)
                {
                    Message msg;
		    if(ps::Postoffice::Get()->van()->HasFeature(Van::MetadataElision))
		    {
			//overwrite timestamp, because if the ps hub isnt initialized, it will 
			//use the old ps which returns whatever timestamp we send it, which in this case is
			//physical key. we want it to return the virtual key instead.
			PopulateMessage(&msg, obj_->id(), true, push, cmd, kvs.keys[i], ps::kServerGroup);
			//printf("worker pumping out key = %d, ts = %d\n", kvs.keys[i], msg.meta.timestamp);
		    }
		    else
		    {
			PopulateMessage(&msg, obj_->id(), true, push, cmd, timestamp, ps::kServerGroup);
		    }
                    msg.AddData(SArray<Key>(1, kvs.keys[i]));
                    //all these keys are linked to the same buffer.
                    if (kvs.lens.size())
                    {
                        CHECK(push);
                        //some tests do not come with lens prepopulated, we mandate all come with lens for push
                        CHECK(kvs.keys.size() == kvs.lens.size());
                        msg.AddData(kvs.vals.segment(cumulativeSizeInFloat, cumulativeSizeInFloat + kvs.lens[i]));
                        cumulativeSizeInFloat += kvs.lens[i];
                        msg.AddData(SArray<int>(1, kvs.lens[i]));

                    }
                    else
                    {
                        //CHECK(kvs.keys.size() == 1) <<" expected key size = 1, but received "<<SummarizeContinousBuffer(kvs.keys.data(), kvs.keys.size()) << " msg is "<<msg.DebugString();
                        CHECK(msg.meta.push == 0);
                        CHECK(kvs.vals.size() == 0);
                        msg.AddData(kvs.vals);
                    }
                    //we should only send this to designated server.
                    //CHECK(false) << "the above change needs to be made";
                    //for(auto id : ps::Postoffice::Get()->GetNodeIDs(ps::kServerGroup))
                    //{
                    //map back to a server.
                    msg.meta.recver = 8 + 2 * Postoffice::Get()->van()->PHUBGetKeySharding(kvs.keys[i]);
                    //printf("STACK = %s\n", GetStacktraceString().c_str());
                    //printf("worker sedning key %d\n", kvs.keys[i]);
                    Postoffice::Get()->van()->Send(msg);
                    //}
                }
            }
            else
            {
                //no key branch? what is this for??
                //mirror what the original code does.
                Message msg;
                PopulateMessage(&msg, obj_->id(), true, push, cmd, timestamp, Postoffice::Get()->ServerRankToID(0));
                Postoffice::Get()->van()->Send(msg);

            }
        }
        else
        {
            // slice the message
            // slice by equally partitioning the key to different servers.
            SlicedKVs sliced;
            slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);

            // need to add response first, since it will not always trigger the callback
            int skipped = 0;
            for (size_t i = 0; i < sliced.size(); ++i) {
                if (!sliced[i].first) ++skipped;
            }
            obj_->AddResponse(timestamp, skipped);
            if ((size_t)skipped == sliced.size()) {
                RunCallback(timestamp);
            }

            for (size_t i = 0; i < sliced.size(); ++i) {
                const auto& s = sliced[i];
                if (!s.first) continue;
                Message msg;
                PopulateMessage(&msg, obj_->id(), true, push, cmd, timestamp, Postoffice::Get()->ServerRankToID(i));
                const auto& kvs = s.second;
                if (kvs.keys.size()) {
                    msg.AddData(kvs.keys);
                    msg.AddData(kvs.vals);
                    if (kvs.lens.size()) {
                        msg.AddData(kvs.lens);
                    }
                    //printf("Blasting out message %s\n", msg.DebugString().c_str());

                }
                //printf("Delivering Key = %d, msg = %s\n", kvs.keys[0], msg.DebugString().c_str());
                Postoffice::Get()->van()->Send(msg);
            }
        }
    }


    template <typename Val>
    void KVWorker<Val>::Process(const Message& msg) {
        if (msg.meta.simple_app) {
            SimpleApp::Process(msg); return;
        }
	bool controlResponse =  
	    msg.meta.head != 0 || //note head 0 is
	    msg.meta.control.empty() == false ||
	    msg.meta.simple_app != false;
        // store the data for pulling
        int ts = msg.meta.timestamp;
	if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false)
	{
	    auto mts = obj_->CurrentTimestamp();
	    CHECK(ts <= mts) << ts << " v.s. " << mts;
	}
	else if(controlResponse == false)
	{
	    //override ts if this is not a control response (GOD)
	    ts = PHUBVirtualToPhysicalKeyMapping.at(ts);
	}
	//printf("received %s\n", msg.DebugString().c_str());
        if (!msg.meta.push && msg.data.size()) {
            CHECK_GE(msg.data.size(), (size_t)2);
            KVPairs<Val> kvs;
            kvs.keys = msg.data[0];
            kvs.vals = msg.data[1];
            if (msg.data.size() > (size_t)2) {
                kvs.lens = msg.data[2];
            }
            mu_.lock();
            recv_kvs_[ts].push_back(kvs);
            mu_.unlock();
        }
        if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::SupportsKeyChunking))
        {
            //printf("process msg = %s, recved = %d. expected = %d\n", msg.DebugString().c_str(), obj_->NumResponse(ts), obj_->ExpectedResponse(ts));
            if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false ||
		controlResponse)
            {
		//printf("message is here instead %s, %d, %d, %d, meta.kempty = %d\n", msg.DebugString().c_str(), msg.meta.head, msg.meta.control.empty(), msg.meta.simple_app, Meta::kEmpty);
                if (obj_->ResponseReady(ts))
                {
                    //printf("running callback ts = %d\n",ts);
                    RunCallback(ts);
                }
            }
            else
            {
		CHECK(msg.meta.head == Control::EMPTY);
		auto effectiveTS = ts;//PHUBVirtualToPhysicalKeyMapping.at(ts);
		
                PHUBDeterministicChunkCounters[effectiveTS] -= 1;

                if (PHUBDeterministicChunkCounters.at(effectiveTS) == 0)
                {
                    PHUBRunCallbackWithDeterministicKey(effectiveTS);
                }
		//printf("PHUBDeterministicChunkCounters.at(ts) == %d\n", PHUBDeterministicChunkCounters.at(ts));
		CHECK(PHUBDeterministicChunkCounters[effectiveTS] >= 0);
		//printf("[UPDATED]PHUBDeterministicChunkCounters.at(%d) == %d, actual ts = %d\n",effectiveTS, PHUBDeterministicChunkCounters.at(effectiveTS), ts);

            }
        }
        else
        {
	    //printf("msg is here %s\n", msg.DebugString().c_str());
            // finished, run callbacks. ORIGINAL code, dont want to touch.
            if (obj_->NumResponse(ts) == Postoffice::Get()->num_servers() - 1)
            {
                RunCallback(ts);
            }
        }

        //printf("msg is handled =%s\n", msg.DebugString().c_str());
    }
    template <typename Val>
    void KVWorker<Val>::RunCallback(int timestamp)
    {
        mu_.lock();
        auto it = callbacks_.find(timestamp);
        if (it != callbacks_.end()) {
            mu_.unlock();
            //printf("[%d][%d]callbacking ts=%d\n",  timestamp);
            CHECK(it->second);
            //printf("truly running callback = %d\n",timestamp);
            it->second();

            mu_.lock();
            callbacks_.erase(it);
        }
        //printf("processing for ts=%d has finished\n", timestamp);
        mu_.unlock();
    }

    template <typename Val>
    template <typename C, typename D>
    int KVWorker<Val>::Pull_(
        const SArray<Key>& keys, C* vals, D* lens, int cmd, const Callback& cb) {
        int ts = -1;
	if(ps::Postoffice::Get()->van()->HasFeature(Van::MetadataElision) == false)
	{
	    ts = ps::Postoffice::Get()->van()->HasFeature(ps::Van::SupportsKeyChunking) ?
		obj_->NewRequestKeyChunk(keys.size()) :
		obj_->NewRequest(kServerGroup);
	}
	else
	{
	    ts = PHUBVirtualToPhysicalKeyMapping.at(keys[0]);
	}
        //printf("[?][1]pulling for key = %d, ts = %d\n", keys[0], ts);
        auto currentCb = [this, ts, keys, vals, lens, cb]() mutable {
            MEASURE_THIS_TIME_BEG(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_FIRST_MUTEX_WAIT, keys[0]);
            mu_.lock();
            auto& kvs = recv_kvs_[ts];
            mu_.unlock();
            MEASURE_THIS_TIME_END(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_FIRST_MUTEX_WAIT, keys[0]);
            MEASURE_THIS_TIME_BEG(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_BODY, keys[0]);
            // do check
            size_t total_key = 0, total_val = 0;
            for (const auto& s : kvs) {
                Range range = FindRange(keys, s.keys.front(), s.keys.back() + 1);
                CHECK_EQ(range.size(), s.keys.size())
                    << "unmatched keys size from one server " << range.size() << " vs " << s.keys.size() << " s.keys.front = " << s.keys.front() << " s.keys.back = " << s.keys.back() << " keys[0] = " << keys[0];
                if (lens) CHECK_EQ(s.lens.size(), s.keys.size());
                total_key += s.keys.size();
                total_val += s.vals.size();
            }
            CHECK_EQ(total_key, keys.size()) << "lost some servers?";

            // fill vals and lens
            std::sort(kvs.begin(), kvs.end(), [](
                const KVPairs<Val>& a, const KVPairs<Val>& b) {
                return a.keys.front() < b.keys.front();
            });
            CHECK_NOTNULL(vals);
            if (vals->empty()) {
                vals->resize(total_val);
            }
            else {
                CHECK_EQ(vals->size(), total_val) << vals->size() << " vs " << total_val;
            }
            Val* p_vals = vals->data();
            int *p_lens = nullptr;
            if (lens) {
                if (lens->empty()) {
                    lens->resize(keys.size());
                }
                else {
                    CHECK_EQ(lens->size(), keys.size());
                }
                p_lens = lens->data();
            }
            //printf("Key = %d, Destination Location = %p, Source Location = %p\n", keys[0], p_vals, kvs[0].vals.data());

            //PHuB should deliver this directly to vals and len locations.
            for (const auto& s : kvs) {
                if (p_vals != s.vals.data())
                {
                    //printf("isn't 0 copy working? \n");
                    memcpy(p_vals, s.vals.data(), s.vals.size() * sizeof(Val));
                }
                //printf("copy of key %d, total key count = %d, 0Copy = %d\n", s.keys[0], s.keys.size() , p_vals == s.vals.data());
                p_vals += s.vals.size();
                if (p_lens) {
                    memcpy(p_lens, s.lens.data(), s.lens.size() * sizeof(int));
                    p_lens += s.lens.size();
                }
            }

            MEASURE_THIS_TIME_END(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_BODY, keys[0]);
            MEASURE_THIS_TIME_BEG(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_SECOND_MUTEX_WAIT, keys[0]);
            mu_.lock();
            recv_kvs_.erase(ts);
            mu_.unlock();
            MEASURE_THIS_TIME_END(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_SECOND_MUTEX_WAIT, keys[0]);
            if (cb) cb();
        };
	CHECK(ts!=-1);
        if (ps::Postoffice::Get()->van()->HasFeature(ps::Van::MetadataElision) == false)
        {
            AddCallback(ts, currentCb);
        }
        else
        {
            PHUBAddCallbackWithDeterministicKey(ts, currentCb);
        }
        KVPairs<Val> kvs; kvs.keys = keys;
        //printf("PULL. Response # for %d is %d\n", ts, obj_->NumResponse(ts));
        Send(ts, false, cmd, kvs);
        return ts;
    }

}  // namespace ps
#endif  // PS_KV_APP_H_
