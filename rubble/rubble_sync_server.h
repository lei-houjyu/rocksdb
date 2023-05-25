#pragma once

#include <iostream>
#include <iomanip>
#include <string> 
#include <memory>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <chrono>
#include <queue>
#include <map>
#include <thread>

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
using rubble::RecoverRequest;
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
  Status Recover(ServerContext* context, const RecoverRequest* request, Empty* reply) override;

  rocksdb::ColumnFamilyData* GetCFD();

  size_t QueuedOpNum();

  size_t QueuedEditNum();

  volatile std::atomic<uint64_t> r_op_counter_{0};
  volatile std::atomic<uint64_t> w_op_counter_{0};

  inline void SpawnBGThreads() {
    if (db_options_->is_rubble && !db_options_->is_primary)
      bg_threads_handle_.SpawnBGThreads();
  }

  inline void FinishBGThreads() {
    if (db_options_->is_rubble && !db_options_->is_primary)
      bg_threads_handle_.Finish();
  }

  private:
    void VersionEditsExecutor();

    // class BGThreadsHandle;
    // friend class BGThreadHandle;

    class BGThreadsHandle {
    private:
      std::thread handle_;
      RubbleKvServiceImpl* rubble_impl_;
    public:   
      BGThreadsHandle(RubbleKvServiceImpl* impl_): rubble_impl_(impl_) {}

      void SpawnBGThreads() {
          handle_ = std::thread([this] {
            rubble_impl_->VersionEditsExecutor();
          });
          pthread_setname_np(handle_.native_handle(), "VersionEditsExecutor");
      }

      void Finish() {
          handle_.join();
      }
    } bg_threads_handle_;

    inline void cached_edits_insert(uint64_t edit_num, std::pair<rocksdb::VersionEdit, std::string> pair) {
      std::lock_guard<std::mutex> lk{*db_options_->version_edit_mu};
      cached_edits_.insert({edit_num, pair});
    }

    inline std::pair<rocksdb::VersionEdit, std::string>& cached_edits_get(uint64_t edit_num) {
      std::lock_guard<std::mutex> lk{*db_options_->version_edit_mu};
      assert(cached_edits_.count(edit_num) == 1);
      auto it = cached_edits_.find(edit_num);
      return it->second;
    }

    inline void cached_edits_remove(uint64_t edit_num) {
      std::lock_guard<std::mutex> lk{*db_options_->version_edit_mu};
      assert(cached_edits_.count(edit_num) == 1);
      auto it = cached_edits_.find(edit_num);
      cached_edits_.erase(it);
    }

    // get the id of the current memtable
    uint64_t get_mem_id();

    void set_mem_id(uint64_t id);

    // check if it's an out-of-ordered write request
    bool is_ooo_write(SingleOp* singleOp);

    // check if it's OK to execute a buffered request
    bool should_execute(uint64_t target_mem_id);

    // actually handle an op request
    void HandleOp(Op* op, OpReply* reply,
                  Forwarder* forwarder, ReplyClient* reply_client,
                  std::map<uint64_t, std::queue<SingleOp*>>* op_buffer);

    void HandleSingleOp(SingleOp* singleOp, Forwarder* forwarder, ReplyClient* reply_client);

    void PostProcessing(SingleOp* singleOp, Forwarder* forwarder, ReplyClient* reply_client);

    // actually handle the SyncRequest
    void HandleSyncRequest(const SyncRequest* request, 
                            SyncReply* reply);

    // calling UpdateSstView and logAndApply
    std::string ApplyVersionEdits(const std::string& args);

    void BufferVersionEdits(const std::string& args);
    
    std::string ApplyOneVersionEdit(std::vector<rocksdb::VersionEdit>& edits);

    void ApplyBufferedVersionEdits();
    
    bool IsReady(const rocksdb::VersionEdit& edit);

    bool IsTermination(Op* op);

    void CleanBufferedOps(Forwarder* forwarder,
                          ReplyClient* reply_client,
                          std::map<uint64_t, std::queue<SingleOp *>> *op_buffer);

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
    std::string SetSyncReplyMessage();
    void SetDoOpReplyMessage(OpReply *reply);

    void PersistData();

    void poll_op_buffer(Forwarder* forwarder, ReplyClient* reply_client,
                          std::map<uint64_t, std::queue<SingleOp*>>* op_buffer);

    void InsertTail();

    void Reconnect();

    void SyncSST();

    // void ApplyDownstreamSstSlotDeletion(const std::vector<int>& deleted_slots);

    // db instance
    rocksdb::DB* db_ = nullptr;
    rocksdb::DBImpl* impl_ = nullptr;
    // db's mutex
    rocksdb::InstrumentedMutex* mu_;
    // db status after processing an operation
    rocksdb::Status s_;
    rocksdb::IOStatus ios_;

    std::shared_ptr<Channel> channel_ = nullptr;
    std::shared_ptr<Channel> primary_channel_ = nullptr;

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

    std::shared_ptr<Edits> edits_;

    // is rubble mode? If set to false, server runs a vanilla rocksdb
    bool is_rubble_ = false;
    bool is_head_ = false;
    bool is_tail_ = false;

    enum RecoveryStatus {
      REMOVING_TAIL,
      INSERTING_TAIL,
      SYNCING_TAIL,
      HEALTHY
    };

    RecoveryStatus recovery_status_ = HEALTHY;

    uint64_t min_mem_id_to_forward_ = 0;

    bool  piggyback_edits_ = false;
  
    std::multimap<uint64_t, std::pair<rocksdb::VersionEdit, std::string>> cached_edits_;

    // id for a Sync Request, assign it to the reply id
    uint64_t request_id_;

    // files that get deleted in a full compaction
    std::vector<uint64_t> deleted_files_;
    
    std::atomic<uint64_t> batch_counter_{0};
    time_point<high_resolution_clock> batch_start_time_;
    time_point<high_resolution_clock> batch_end_time_;
    std::thread status_thread_;
    std::map< std::thread::id, std::map< uint64_t, std::queue<SingleOp*> >* > buffers_;
    std::mutex deleted_slots_mu_;
    std::unordered_set<int> deleted_slots_;
};