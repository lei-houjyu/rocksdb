#pragma once

#include <iostream>
#include <iomanip>
#include <string> 
#include <memory>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"
#include "reply_client.h"
#include "forwarder.h"

#include "rocksdb/db.h"
#include "port/port_posix.h"
#include "port/port.h"
#include "db/version_edit.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using rubble::RubbleKvStoreService;
using rubble::SyncRequest;
using rubble::SyncReply;
using rubble::Op;
using rubble::OpReply;
using rubble::SingleOp;
using rubble::SingleOpReply;
using rubble::OpType_Name;
using rubble::PingRequest;
using rubble::Empty;

using json = nlohmann::json;
using std::chrono::time_point;
using std::chrono::high_resolution_clock;

// This service is serving DoOp and Sync both synchronously
class RubbleKvServiceImpl final : public  RubbleKvStoreService::Service {
  public:
    explicit RubbleKvServiceImpl(rocksdb::DB* db);

    ~RubbleKvServiceImpl();

  // synchronous version of DoOp
  Status DoOp(ServerContext* context, 
              ServerReaderWriter<OpReply, Op>* stream) override ;


  // a streaming RPC used by the non-tail node to sync Version(view of sst files) states to the downstream node 
  Status Sync(ServerContext* context, 
              ServerReaderWriter<SyncReply, SyncRequest>* stream) override;
  
  // heartbeat between Replicator and db servers
  Status Pulse(ServerContext* context, const PingRequest* request, Empty* reply) override;

  volatile std::atomic<uint64_t> r_op_counter_{0};
  volatile std::atomic<uint64_t> w_op_counter_{0};

  private:
    // actually handle an op request
    void HandleOp(const Op& op, OpReply* reply);

    // actually handle the SyncRequest
    void HandleSyncRequest(const SyncRequest* request, 
                            SyncReply* reply);

    // calling UpdateSstView and logAndApply
    std::string ApplyVersionEdits(const std::string& args);
    
    std::string ApplyOneVersionEdit(std::vector<rocksdb::VersionEdit>& edits);

    // parse the version edit json string to rocksdb::VersionEdit 
    rocksdb::VersionEdit ParseJsonStringToVersionEdit(const json& j_edit /* json version edit */);

    //called by secondary nodes to create a pool of preallocated ssts in rubble mode
    rocksdb::IOStatus CreateSstPool();

    // In a 3-node setting, if it's the second node in the chain it should also ship sst files it received from the primary/first node
    // to the tail/downstream node and also delete the ones that get deleted in the compaction
    // for non-head node, should update sst bit map
    // since second node's flush is disabled ,we should do the shipping here when it received Sync rpc call from the primary
    /**
     * @param edit The version edit received from the priamry 
     * 
     */
    rocksdb::IOStatus UpdateSstViewAndShipSstFiles(const rocksdb::VersionEdit& edit);
    rocksdb::IOStatus DeleteSstFiles(const rocksdb::VersionEdit& edit);
    // set the reply message according to the status
    void SetReplyMessage(SyncReply* reply, const rocksdb::Status& s, bool is_flush, bool is_trivial_move);

    // db instance
    rocksdb::DB* db_ = nullptr;
    rocksdb::DBImpl* impl_ = nullptr;
    // db's mutex
    rocksdb::InstrumentedMutex* mu_;
    // db status after processing an operation
    rocksdb::Status s_;
    rocksdb::IOStatus ios_;

    std::shared_ptr<Channel> channel_ = nullptr;

    std::shared_ptr<Forwarder> forwarder_ = nullptr;
    // client for sending the reply back to the replicator
    std::shared_ptr<ReplyClient> reply_client_ = nullptr;

    // rocksdb's version set
    rocksdb::VersionSet* version_set_;

    rocksdb::ColumnFamilySet* column_family_set_;
    // db's default columnfamily data 
    rocksdb::ColumnFamilyData* default_cf_;
    // rocksdb internal immutable db options
    const rocksdb::ImmutableDBOptions* db_options_;
    // rocksdb internal immutable column family options
    const rocksdb::ImmutableCFOptions* ioptions_;
    const rocksdb::MutableCFOptions* cf_options_;

    rocksdb::Cache* table_cache_;

    std::shared_ptr<rocksdb::Logger> logger_ = nullptr;
  
    // right now, just put all sst files under one path
    rocksdb::DbPath db_path_;

    // maintain a mapping between sst_number and slot_number
    // sst_bit_map_[i] = j means sst_file with number j occupies the i-th slot
    // secondary node will update it when received a Sync rpc call from the upstream node
    std::unordered_map<int, uint64_t> sst_bit_map_;
    
    rocksdb::FileSystem* fs_;

    std::atomic<uint64_t> log_apply_counter_{0};

    // client for making Sync rpc call to downstream node
    std::shared_ptr<SyncClient> sync_client_;

    std::shared_ptr<Edits> edits_;

    // is rubble mode? If set to false, server runs a vanilla rocksdb
    bool is_rubble_ = false;
    bool is_head_ = false;
    bool is_tail_ = false;

    bool  piggyback_edits_ = false;
  
    std::atomic<uint64_t> version_edit_id_{0};
    std::multimap<uint64_t, rocksdb::VersionEdit> cached_edits_;

    // id for a Sync Request, assign it to the reply id
    uint64_t request_id_;

    // files that get deleted in a full compaction
    std::vector<uint64_t> deleted_files_;
    
    std::atomic<uint64_t> batch_counter_{0};
    time_point<high_resolution_clock> batch_start_time_;
    time_point<high_resolution_clock> batch_end_time_;
    std::thread status_thread_;
};


