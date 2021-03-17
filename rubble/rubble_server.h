#pragma once

#include <iostream>
#include <string>
#include <sstream>   
#include <memory>
#include <typeinfo> 
#include <nlohmann/json.hpp>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"

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
using grpc::ServerCompletionQueue;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerReaderWriter;
using grpc::ServerAsyncReaderWriter;
using grpc::Status;
using grpc::StatusCode;

using rubble::RubbleKvStoreService;
using rubble::SyncRequest;
using rubble::SyncReply;
using rubble::Op;
using rubble::OpReply;

using json = nlohmann::json;

int g_thread_num = 1;
int g_cq_num = 1;
int g_pool = 1;

class CallDataBase {
public:
  CallDataBase(RubbleKvStoreService::AsyncService* service, ServerCompletionQueue* cq, rocksdb::DB* db)
   :service_(service), cq_(cq), db_(db){
      rocksdb::DBImpl* impl = (rocksdb::DBImpl*)db_;
      auto version_set = impl->TEST_GetVersionSet();
      db_options_ = version_set->db_options();
      kvstore_client_ = db_options->kvstore_client;
   }

  virtual void Proceed(bool ok) = 0;

  virtual void HandleOp() = 0;

protected:

  // db instance
  rocksdb::DB* db_;

  // status of the db after performing an operation.
  rocksdb::Status s_;

  rocksdb::ImmutableDBOptions* db_options_;

  std::shared_ptr<KvStoreClient> kvstore_client_; 
  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  RubbleKvStoreService::AsyncService* service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue* cq_;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // What we get from the client.
  Op request_;
  // What we send back to the client.
  OpReply reply_;

};

// CallData for Bidirectional streaming rpc 
class CallDataBidi : CallDataBase {

 public:

  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  CallDataBidi(RubbleKvStoreService::AsyncService* service, ServerCompletionQueue* cq, rocksdb::DB* db)
   :CallDataBase(service, cq, db),rw_(&ctx_){
    // Invoke the serving logic right away.

    status_ = BidiStatus::CONNECT;

    ctx_.AsyncNotifyWhenDone((void*)this);

    // As part of the initial CREATE state, we *request* that the system
    // start processing DoOp requests. In this request, "this" acts are
    // the tag uniquely identifying the request (so that different CallData
    // instances can serve different requests concurrently), in this case
    // the memory address of this CallData instance.
    service_->RequestDoOp(&ctx_, &rw_, cq_, cq_, (void*)this);
  }
  // async version of DoOp
  void Proceed(bool ok) override {

    std::unique_lock<std::mutex> _wlock(this->m_mutex);

    switch (status_) {
    case BidiStatus::READ:

        //Meaning client said it wants to end the stream either by a 'writedone' or 'finish' call.
        if (!ok) {
            std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " CQ returned false." << std::endl;
            Status _st(StatusCode::OUT_OF_RANGE,"test error msg");
            // rw_.Write(reply_, (void*)this);
            rw_.Finish(_st,(void*)this);
            status_ = BidiStatus::DONE;
            std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " after call Finish(), cancelled:" << this->ctx_.IsCancelled() << std::endl;
            break;
        }

        rw_.Read(&request_, (void*)this);
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " Received a new " << request_.type() << " op " << std::endl;

        // Handle a db operation
        HandleOp();
        // assert(db_options.is_ruuble);
        // Forward the request to the downstream node in the chain if it's not a tail node
        if(db_options.is_ruuble && !db_options_.is_tail){
          kvstore_client_->SetRequest(request_);
          kvstore_client_->AsynDoOp();
        }else if(db_options.is_ruuble &&db_options_.is_tail) {
          // tail node
          // TODO: send the true reply back to replicator
          
        }

        if(request_.type() == Op::GET){
          // if it's a Get Op, should return reply back to the client
          status_ = BidiStatus::WRITE;
        }else{
          status_ = BidiStatus::READ;
        }
        break;

    case BidiStatus::WRITE:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " Get a value : " << reply_.value() << " for key : " << reply_.key() << std::endl;
        rw_.Write(reply_, (void*)this);
        status_ = BidiStatus::READ;
        break;

    case BidiStatus::CONNECT:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this << " connected:" << std::endl;
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new CallDataBidi(service_, cq_, db_);
        rw_.Read(&request_, (void*)this);
        status_ = BidiStatus::READ;
        break;

    case BidiStatus::DONE:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this
                << " Server done, cancelled:" << this->ctx_.IsCancelled() << std::endl;
        status_ = BidiStatus::FINISH;
        break;

    case BidiStatus::FINISH:
        std::cout << "thread:" << std::this_thread::get_id() <<  "tag:" << this << " Server finish, cancelled:" << this->ctx_.IsCancelled() << std::endl;
        _wlock.unlock();
        delete this;
        break;

    default:
        std::cerr << "Unexpected tag " << int(status_) << std::endl;
        assert(false);
    }
  }

 private:

  void HandleOp() override {
      std::string value;

      switch (request_.type())
      {
      case Op::GET:
        s_ = db_->Get(rocksdb::ReadOptions(), request_.key(), &value);
        reply_.set_type(Op::GET);
        reply_.set_status(s_.ToString());
        if(s_.ok()){
          reply_.set_ok(true);
          reply_.set_value(value);
        }else{
          reply_.set_ok(false);
        }
        break;

      case Op::PUT:
        s_ = db_->Put(rocksdb::WriteOptions(), request_.key(), request_.value());
        reply_.set_type(Op::PUT);
        if(s_.ok()){
          reply_.set_ok(true);
        }else{
          reply_.set_ok(false);
          reply_.set_status(s_.ToString());
        }
        break;

      case Op::DELETE:
        //TODO
        break;

      case Op::UPDATE:
        //TODO
        break;

      default:
        std::cerr << "Unsupported Operation \n";
        break;
      }
  }

  // The means to get back to the client.
  ServerAsyncReaderWriter<OpReply, Op>  rw_;

  // Let's implement a tiny state machine with the following states.
  enum class BidiStatus { READ = 1, WRITE = 2, CONNECT = 3, DONE = 4, FINISH = 5 };
  BidiStatus status_;

  std::mutex   m_mutex;
};

class RubbleKvServiceImpl final : public RubbleKvStoreService::Service {
  public:
    explicit RubbleKvServiceImpl(rocksdb::DB* db)
      :db_(db), impl_((rocksdb::DBImpl*)db_), 
       mu_(impl_->mutex()),
       version_set_(impl_->TEST_GetVersionSet()),
       db_options_(version_set_->db_options()),
       is_rubble_(db_options_.is_rubble),
       is_head_(db_options_.is_primary),
       is_tail_(db_options_.is_tail),
       sync_client_(db_options_.sync_client),
       kvstore_client_(db_options_.kvstore_client),
       column_family_set_(version_set_->GetColumnFamilySet()),
       default_cf_(column_family_set_->GetDefault()),
       ioptions_(default_cf_->ioptions()),
       fs_(ioptions_->fs){
          
    };

    ~RubbleKvServiceImpl() {
      delete db_;
    }

  void ParseJsonStringToVersionEdit(const json& j /* json version edit */, rocksdb::VersionEdit* edit){

      // std::cout << "Dumped VersionEdit : " << j.dump(4) << std::endl;

      assert(j.contains("AddedFiles"));
      if(j.contains("IsFlush")){ // means edit corresponds to a flush job
        is_flush_ = true;
        num_of_added_files_ = j["AddedFiles"].get<std::vector<json>>().size();
        auto added_file = j["AddedFiles"].get<std::vector<json>>().front();
        added_file_num_ = added_file["FileNumber"].get<uint64_t>();
        batch_count_ = j["BatchCount"].get<int>();
      }

      if(j.contains("LogNumber")){
        edit->SetLogNumber(j["LogNumber"].get<uint64_t>());
      }

      if(j.contains("PrevLogNumber")){
        edit->SetPrevLogNumber(j["PrevLogNumber"].get<uint64_t>());
      }
      assert(!j["ColumnFamily"].is_null());
      edit->SetColumnFamily(j["ColumnFamily"].get<uint32_t>());

      int max_file_num = 0;
     
      for(auto& j_added_file : j["AddedFiles"].get<std::vector<json>>()){
          
          assert(!j_added_file["SmallestUserKey"].is_null());
          assert(!j_added_file["SmallestSeqno"].is_null());
          // TODO: should decide ValueType according to the info in the received version edit 
          // basically pass the ValueType of the primary's version Edit's smallest/largest InterKey's ValueType
          rocksdb::InternalKey smallest(rocksdb::Slice(j_added_file["SmallestUserKey"].get<std::string>()), 
                                          j_added_file["SmallestSeqno"].get<uint64_t>(),rocksdb::ValueType::kTypeValue);

          assert(smallest.Valid());
  
          uint64_t smallest_seqno = j_added_file["SmallestSeqno"].get<uint64_t>();
         
          assert(!j_added_file["LargestUserKey"].is_null());
          assert(!j_added_file["LargestSeqno"].is_null());
          rocksdb::InternalKey largest(rocksdb::Slice(j_added_file["LargestUserKey"].get<std::string>()), 
                                          j_added_file["LargestSeqno"].get<uint64_t>(),rocksdb::ValueType::kTypeValue);

          assert(largest.Valid());

          uint64_t largest_seqno = j_added_file["LargestSeqno"].get<uint64_t>();

          int level = j_added_file["Level"].get<int>();

          uint64_t file_num = j_added_file["FileNumber"].get<uint64_t>();
          max_file_num = std::max(max_file_num, (int)file_num);

          uint64_t file_size = j_added_file["FileSize"].get<uint64_t>();

          edit->AddFile(level, file_num, 0 /* path_id shoule be 0*/,
                      file_size, 
                      smallest, largest, 
                      smallest_seqno, largest_seqno,
                      false, 
                      rocksdb::kInvalidBlobFileNumber,
                      rocksdb::kUnknownOldestAncesterTime,
                      rocksdb::kUnknownFileCreationTime,
                      rocksdb::kUnknownFileChecksum, 
                      rocksdb::kUnknownFileChecksumFuncName
                      );
      }
      next_file_num_ = max_file_num + 1;

      if(j.contains("DeletedFiles")){
        for(auto j_delete_file : j["DeletedFiles"].get<std::vector<json>>()){
          edit->DeleteFile(j_delete_file["Level"].get<int>(), j_delete_file["FileNumber"].get<uint64_t>());
        }
      }
  }


  //an Unary RPC call used by the non-tail node to sync Version(view of sst files) states to the downstream node 
  Status Sync(ServerContext* context, const SyncRequest* request, 
                          SyncReply* reply) override {
    log_apply_counter_++;
    std::cout << " --------[Secondary] Accepting Sync RPC " << log_apply_counter_.load() << " th times --------- \n";
    rocksdb::VersionEdit edit;
    /**
     * example args json: 
     * 
     *  {
     *     "AddedFiles": [
     *          {
     *             "FileNumber": 12,
     *             "FileSize": 71819,
     *             "LargestSeqno": 7173,
     *             "LargestUserKey": "key7172",
     *             "Level": 0,
     *             "SmallestSeqno": 3607,
     *             "SmallestUserKey": "key3606"
     *          }
     *      ],
     *      "ColumnFamily": 0,
     *      "DeletedFiles": [
     *          {
     *              "FileNumber": 8,
     *              "Level": 0
     *          },
     *          {
     *              "FileNumber": 12,
     *              "Level": 0
     *          }
     *       ],
     *      "EditNumber": 2,
     *      "LogNumber": 11,
     *      "PrevLogNumber": 0,
     * 
     *       // fields exist only when it's triggered by a flush
     *      "IsFlush" : 1,
     *      "BatchCount" : 2
     *  }
     *  
     */ 
    
    std::string args = request->args();
    const json j_args = json::parse(args);
    ParseJsonStringToVersionEdit(j_args, &edit);

    rocksdb::Status s;

    // If it's neither primary/head nor tail(second node in the chain in a 3-node setting)
    // should call Sync rpc to downstream node also should ship sst files to the downstream node
    // assert(is_rubble_);
    if(is_rubble_ && !is_head_ && ! is_tail_){
      s = ShipSstFiles(edit);
      std::string sync_reply = db_options_->sync_client->Sync(*request);
      std::cout << "[ Reply Status ]: " << sync_reply << std::endl;
    }
 
    // logAndApply needs to hold the mutex
    rocksdb::InstrumentedMutexLock l(mu_);
    // std::cout << " --------------- mutex lock ----------------- \n ";

    uint64_t current_next_file_num = version_set_->current_next_file_number();
    
    const rocksdb::MutableCFOptions* cf_options = default_cf_->GetCurrentMutableCFOptions();

    rocksdb::autovector<rocksdb::ColumnFamilyData*> cfds;
    cfds.emplace_back(default_cf_);

    rocksdb::autovector<const rocksdb::MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(cf_options);

    rocksdb::autovector<rocksdb::VersionEdit*> edit_list;
    edit_list.emplace_back(&edit);

    rocksdb::autovector<rocksdb::autovector<rocksdb::VersionEdit*>> edit_lists;
    edit_lists.emplace_back(edit_list);

    rocksdb::FSDirectory* db_directory = impl_->directories_.GetDbDir();
    rocksdb::MemTableList* imm = default_cf_->imm();
    
    // std::cout << "MemTable : " <<  json::parse(default_cf->mem()->DebugJson()).dump(4) << std::endl;
    std::cout  << "Immutable MemTable list : " << json::parse(imm->DebugJson()).dump(4) << std::endl;
    std::cout << " Current Version :\n " << default_cf->current()->DebugString(false) <<std::endl;

    // set secondary version set's next file num according to the primary's next_file_num_
    version_set_->FetchAddFileNumber(next_file_num_ - current_next_file_num);
    assert(version_set_->current_next_file_number() == next_file_num_);

    // Calling LogAndApply on the secondary
    s = version_set_->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu_,
                      db_directory);


    //Drop The corresponding MemTables in the Immutable MemTable List
    //If this version edit corresponds to a flush job
    if(is_flush_){
      //flush always output one file?
      assert(num_of_added_files_ == 1);
      // batch count is always 2 ?
      assert(batch_count_ == 2);
      // creating a new verion after we applied the edit
      imm->InstallNewVersion();

      // All the later memtables that have the same filenum
      // are part of the same batch. They can be committed now.
      uint64_t mem_id = 1;  // how many memtables have been flushed.
      
      rocksdb::autovector<rocksdb::MemTable*> to_delete;
      if(s.ok() &&!default_cf_->IsDropped()){

        while(num_of_added_files-- > 0){
            rocksdb::SuperVersion* sv = default_cf_->GetSuperVersion();
          
            rocksdb::MemTableListVersion*  current = imm->current();
            rocksdb::MemTable* m = current->GetMemlist().back();

            m->SetFlushCompleted(true);
            m->SetFileNumber(added_file_num_); 

            // if (m->GetEdits().GetBlobFileAdditions().empty()) {
            //   ROCKS_LOG_BUFFER(log_buffer,
            //                   "[%s] Level-0 commit table #%" PRIu64
            //                   ": memtable #%" PRIu64 " done",
            //                   cfd->GetName().c_str(), m->file_number_, mem_id);
            // } else {
            //   ROCKS_LOG_BUFFER(log_buffer,
            //                   "[%s] Level-0 commit table #%" PRIu64
            //                   " (+%zu blob files)"
            //                   ": memtable #%" PRIu64 " done",
            //                   cfd->GetName().c_str(), m->file_number_,
            //                   m->edit_.GetBlobFileAdditions().size(), mem_id);
            // }

            // assert(imm->current()->GetMemlist().size() >= batch_count) ? 
            // This is not always the case, sometimes secondary has only one immutable memtable in the list, say ID 89,
            // while the primary has 2 immutable memtables, say 89 and 90, with a more latest one,
            // so should set the number_of_immutable_memtable_to_delete to be the minimum of batch count and immutable memlist size
            int num_of_imm_to_delete = std::min(batch_count_, (int)imm->current()->GetMemlist().size());

            assert(m->GetFileNumber() > 0);
            while(num_of_imm_to_delete-- > 0){
              /* drop the corresponding immutable memtable in the list if version edit corresponds to a flush */
              // according the code comment in the MemTableList class : "The memtables are flushed to L0 as soon as possible and in any order." 
              // as far as I observe, it's always the back of the imm memlist gets flushed first, which is the earliest memtable
              // so here we always drop the memtable in the back of the list
              current->RemoveLast(sv->GetToDelete());
            }

            imm->SetNumFlushNotStarted(current->GetMemlist().size());
            imm->UpdateCachedValuesFromMemTableListVersion();
            imm->ResetTrimHistoryNeeded();
            ++mem_id;
        }
      }else {
        //TODO : Commit Failed For Some reason, need to reset state
        std::cout << s.ToString() << std::endl;
      }

      imm->SetCommitInProgress(false);
      std::cout << " ----------- After RemoveLast : ( ImmutableList : " << json::parse(imm->DebugJson()).dump(4) << " ) ----------------\n";
      int size = static_cast<int> (imm->current()->GetMemlist().size());
      // std::cout << " memlist size : " << size << " , num_flush_not_started : " << imm->GetNumFlushNotStarted() << std::endl;
    }else { // It is either a trivial move compaction or a full compaction

    }

    if(s.ok()){
      reply->set_message("Succeeds");
    }else{
      std::string failed = "Failed : " + s.ToString();
      reply->set_message(failed);
    }

    rocksdb::VersionStorageInfo::LevelSummaryStorage tmp;

    auto vstorage = default_cf_->current()->storage_info();
    // const char* c = vstorage->LevelSummary(&tmp);
    // std::cout << " VersionStorageInfo->LevelSummary : " << std::string(c) << std::endl;

    return Status::OK;
  }

  // In a 3-node setting, if it's the second node in the chain it should also ship sst files it received from the primary/first node
  // to the tail/downstream node and also delete the ones that get deleted in the compaction
  // since second node's flush is disabled ,we should do the shipping here when it received Sync rpc call from the primary
  /**
   * @param edit The version edit received from the priamry 
   * 
   */
  rocksdb::Status ShipSstFiles(rocksdb::VersionEdit& edit){

    assert(db_options_.remote_sst_dir != "");
    std::string remote_sst_dir = db_options_.remote_sst_dir;
    if(remote_sst_dir[remote_sst_dir.length() - 1] != '/'){
        remote_sst_dir = remote_sst_dir + '/';
    }

    rocksdb::IOStatus ios;
    for(const auto& new_file: edit->GetNewFiles()){
      const FileMetaData& meta = new_file.second;
      std::string sst_num = std::to_string(meta.fd.GetNumber());
      std::string sst_file_name = std::string("000000").replace(6 - sst_number.length(), sst_number.length(), sst_number) + ".sst";

      std::string fname = rocksdb::TableFileName(ioptions->cf_paths,
                      meta.fd.GetNumber(), meta.fd.GetPathId());

      std::string remote_sst_fname = remote_sst_dir + sst_file_name;
      ios = CopyFile(fs_.get(), fname, remote_sst_name, 0,  true);

      if (!ios.ok()){
        std::cerr << "[ File Ship Failed ] : " << meta.fd.GetNumber() << std::endl;
      }else {
        std::cout << "[ File Shipped ] : " << meta.fd.GetNumber() << std::endl;
      }
    }

    for(const auto& delete_file : edit->GetDeletedFiles()){
      std::string file_number = std::to_string(delete_file.second);
      std::string sst_file_name = std::string("000000").replace(6 - file_number.length(), file_number.length(), sst_number) + ".sst";

      std::string remote_sst_fname = remote_sst_dir + sst_file_name;
      ios = fs_->FileExists(remote_sst_fname, rocksdb::IOOptions(), nullptr);
      if (ios.ok()){
        ios = fs_->DeleteFile(remote_sst_fname, rocksdb::IOOptions(), nullptr);
        
        if(ios.IsIOError()){
          std::cerr << "[ File Deletion Failed ]:" <<  file_number << std::endl;
        }else if(ios.ok()){
          std::cout << "[ File Deleted ] : " <<  file_number << std::endl;
        }
      }else {
        if (ios.IsNotFound()){
          std::cerr << "file :" << file_number << "does not exist \n";
        }
      }
    }
    return rocksdb::Status:OK();
  }


  // synchronous version of DoOp
  Status DoOp(ServerContext* context, 
              ServerReaderWriter<OpReply, Op>* stream) override {
      std::string value;

      while ( stream->Read(&request_)){
        switch (request_.type())
        {
        case Op::GET:
          s_ = db_->Get(rocksdb::ReadOptions(), request_.key(), &value);
          if(s_.ok()){
            // find the value for the key
            reply_.set_value(value);
            reply_.set_ok(true);
          }else {
            reply_.set_ok(false);
            reply_.set_status(s_.ToString());
          }
          // For a Get op, we return a reply back to the client
          stream->Write(reply_);
          break;
        
        case Op::PUT:
          s_ = db_->Put(rocksdb::WriteOptions(), request_.key(), request_.value());
          ssert(is_rubble_);
          if(!is_tail_){
            kvstore_client_->SyncDoPut(std::pair<std::string, std::string>{request_.key(), request_.value()});
          }else{
            // TODO: tail node should be responsible for sending the true reply back to replicator

          }

          break;
        
        case Op::DELETE:
          //TODO:
          break;
        
        case Op::UPDATE:
          //TODO:
          break;

        default:
        std::cerr << "Unsupported Operation \n";
          break;
        }
      }

      return Status::OK;
  }


  private:
    // a db op request we get from the client
    Op request_;
    //reply we get back to the client for a db op
    OpReply reply_;

    // db instance
    rocksdb::DB* db_ = nullptr;
    rocksdb::DBImpl* impl_ = nullptr;
    // db's mutex
    rocksdb::InstrumentedMutex* mu_;
    // db status after processing an operation
    rocksdb::Status s_;

    // rocksdb's version set
    rocksdb::VersionSet* version_set_;

    rocksdb::ColumnFamilySet* column_family_set_;
    // db's default columnfamily data 
    rocksdb::ColumnFamilyData* default_cf_;
    // rocksdb internal immutable db options
    const rocksdb::ImmutableDBOptions* db_options_;
    // rocksdb internal immutable column family options
    const rocksdb::ImmutableCFOptions* ioptions_;

    rocksdb::FileSystemPtr fs_;

    std::atomic<uint64_t> log_apply_counter_;

    // client for making Sync rpc call to downstream node
    std::shared_ptr<SyncClient> sync_client_;
    // client for forwarding op to downstream node
    std::shared_ptr<KvStoreClient> kvstore_client_;

    // is rubble mode? If set to false, server runs a vanilla rocksdb
    bool is_rubble_ = false;
    // indiacting whether this is the head node in the chain, set to true if db_options_.is_primary is true
    bool is_head_ = false;
    // whether this node is the tail in the chain 
    bool is_tail_ = false;

    /* variables below are used for Sync method */
    // if true, means version edit received indicates a flush job
    bool is_flush_ = false; 
    // number of added sst files
    int num_of_added_files_ = 0;
    int added_file_num_ = 0;
    // number of memtables get flushed in a flush job, looks like is always 2
    int batch_count_ = 0;
    // get the next file num of secondary, which is the maximum file number of the AddedFiles in the shipped vesion edit plus 1
    int next_file_num_ = 0;
};

class ServerImpl final {
  public:
  ServerImpl(ServerBuilder& builder, Rocksdb::DB* db)
   :builder_(builder), db_(db){
  }
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (const auto& _cq : m_cq)
        _cq->Shutdown();
  }

  // There is no shutdown handling in this code.
  void Run() {
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder_.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.

    for (int i = 0; i < g_cq_num; ++i) {
        //cq_ = builder.AddCompletionQueue();
        m_cq.emplace_back(builder.AddCompletionQueue());
    }

    // Finally assemble the server.
    server_ = builder_.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    std::vector<std::thread*> _vec_threads;

    for (int i = 0; i < g_thread_num; ++i) {
        int _cq_idx = i % g_cq_num;
        for (int j = 0; j < g_pool; ++j) {
            new CallDataBidi(&service_, m_cq[_cq_idx].get(), db_);
        }

        _vec_threads.emplace_back(new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx));
    }

    std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

    for (const auto& _t : _vec_threads)
        _t->join();
  }

 private:

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_idx) {
    // Spawn a new CallDataUnary instance to serve new clients.
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallDataUnary instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(m_cq[cq_idx]->Next(&tag, &ok));

      CallDataBase* _p_ins = (CallDataBase*)tag;
      _p_ins->Proceed(ok);
    }
  }

  std::vector<std::unique_ptr<ServerCompletionQueue>>  m_cq;

  RubbleKvStoreService::AsyncService service_;
  std::unique_ptr<Server> server_;
  ServerBuilder builder_;
  rocksdb::DB* db_;
};

void RunServer(rocksdb::DB* db, const std::string& server_addr) {
  
  RubbleKvServiceImpl service(db);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;

  builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
  std::cout << "Server listening on " << server_addr << std::endl;
  builder.RegisterService(&service);
  ServerImpl server_impl(builder, db);
  server_impl.Run();
  // std::unique_ptr<Server> server(builder.BuildAndStart());
  // server->Wait();
}