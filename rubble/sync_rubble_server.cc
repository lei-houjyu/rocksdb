#include "sync_rubble_server.h"
#include <atomic>

RubbleKvServiceImpl::RubbleKvServiceImpl(rocksdb::DB* db)
      :db_(db), impl_((rocksdb::DBImpl*)db_), 
       mu_(impl_->mutex()),
       version_set_(impl_->TEST_GetVersionSet()),
       db_options_(version_set_->db_options()),
       is_rubble_(db_options_->is_rubble),
       is_head_(db_options_->is_primary),
       is_tail_(db_options_->is_tail),
       piggyback_edits_(db_options_->piggyback_version_edits),
       edits_(db_options_->edits),
       db_path_(db_options_->db_paths.front()),
       sync_client_(db_options_->sync_client),
       column_family_set_(version_set_->GetColumnFamilySet()),
       default_cf_(column_family_set_->GetDefault()),
       ioptions_(default_cf_->ioptions()),
       cf_options_(default_cf_->GetCurrentMutableCFOptions()),
       fs_(ioptions_->fs){
        if(is_rubble_ && !is_head_){
          std::cout << "init sync_service --- is_rubble: " << is_rubble_ << "; is_head: " << is_head_ << std::endl;
          ios_ = CreateSstPool();
          if(!ios_.ok()){
            std::cout << "allocate sst pool failed \n";
            assert(false);
          }
          std::cout << "[secondary] sst pool allocation finished" << std::endl;
        }
        std::cout << "target address : " << db_options_->target_address << std::endl;
        if(db_options_->target_address != ""){
          channel_ = db_options_->channel;
          assert(channel_ != nullptr);
        }
    };

RubbleKvServiceImpl::~RubbleKvServiceImpl(){
    delete db_;
}

static std::atomic<int> op_counter{0};
static std::atomic<int> reply_counter{0};
Status RubbleKvServiceImpl::DoOp(ServerContext* context, 
              ServerReaderWriter<OpReply, Op>* stream) {
    // if(!op_counter_.load()){
    //     start_time_ = high_resolution_clock::now();
    // }

    // if(op_counter_.load() && op_counter_.load()%100000 == 0){
    //     end_time_ = high_resolution_clock::now();
    //     auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_);
    //     // why this doesn't print anything?
    //     std::cout << "Throughput : handle 100000  in " << millisecs.count() << " milisecs\n";
    //     start_time_ = end_time_;
    // }

    /* chain replication */
    // Forward the request to the downstream node in the chain if it's not a tail node

    std::shared_ptr<Forwarder> forwarder = nullptr;
    // client for sending the reply back to the replicator
    std::shared_ptr<ReplyClient> reply_client = nullptr;
    if(!db_options_->is_tail){
      std::cout << "init the forwarder" << "\n";
      forwarder = std::make_shared<Forwarder>(channel_);
      // std::cout << "thread: " << map[std::this_thread::get_id()] << " Forwarded " << op_counter_ << " ops" << std::endl;
    }else {
      // tail node should be responsible for sending the reply back to replicator
      // use the sync stream to write the reply back
      if(channel_ != nullptr){
        std::cout << "init the reply client" << "\n";
        reply_client = std::make_shared<ReplyClient>(channel_);
      }
    }

    Op request;
    OpReply reply;
    while (stream->Read(&request)){
      op_counter_.fetch_add(1);

      // if there is version edits piggybacked in the DoOp request, apply those edits first
      if(request.has_edits()){
        for(int i = 0; i < request.edits_size(); i++){
          std::string edit = request.edits(i);
          ApplyVersionEdits(edit);
        }
      }

      // handle DoOp request
      HandleOp(request, &reply);

      if(forwarder != nullptr){
        op_counter.fetch_add(1);
        std::cout << "Forwarded " << op_counter.load() << " ops\n";
        if(piggyback_edits_ && is_rubble_ && is_head_){
          std::vector<std::string> edits;
          edits_->GetEdits(edits);
          if(edits.size() != 0){
            std::cout << "[primary] Got " << edits.size() << " new version edits\n"; 
            // assert(edits.size() == 1);
            request.set_has_edits(true);
            for(auto edit : edits){
              request.add_edits(std::move(edit));
            }
          }else{
            request.set_has_edits(false);
          }
        }
        forwarder->Forward(request);
      }
      
      if(reply_client != nullptr){
        reply_counter.fetch_add(1);
        std::cout << "Sent out " << reply_counter.load() << " opreplies \n";
        reply_client->SendReply(reply);
      }

      // stream->Write(reply);
    }

    return Status::OK;
}

 void RubbleKvServiceImpl::HandleOp(const Op& op, OpReply* reply) {
    // if(!op_counter_.load()){
    //   start_time_ = high_resolution_clock::now();
    // }

    // if(op_counter_.load() && op_counter_.load()%100000 == 0 ){
    //   std::cout << "opcount: " << op_counter_.load() << "\n";
    //   end_time_ = high_resolution_clock::now();
    //   auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_);
    //   std::cout << "Throughput : handled 100000 ops in " << std::to_string(millisecs.count()) << " millisecs\n";
    //   start_time_ = end_time_;
    // }
    
    // std::cout << "handling a " << request_.type() <<" " <<  request_.id() << " op...\n";
    // ASSUME that each batch is with the same type of operation
    assert(op.ops_size() > 0);

    std::string value;
    SingleOpReply* singleOpReply;
    reply->clear_replies();
    rocksdb::Status s;
    // std::cout << "thread: " << map[std::this_thread::get_id()] << " op_counter: " << op_counter_ << std::endl;
    //     <<  " first key in batch: " << op.ops(0).key() << " size: " << op.ops_size() << "\n";
    switch (op.ops(0).type())
    {
      case SingleOp::GET:
        for(const auto& singleOp: op.ops()) {
          singleOpReply = reply->add_replies();
          singleOpReply->set_id(singleOp.id());
          s = db_->Get(rocksdb::ReadOptions(), singleOp.key(), &value);
          singleOpReply->set_key(singleOp.key());
          singleOpReply->set_type(SingleOpReply::GET);
        //   singleOpReply->set_status(s.ToString());
          if(s.ok()){
            singleOpReply->set_ok(true);
            singleOpReply->set_value(value);
          }else{
            singleOpReply->set_ok(false);
            singleOpReply->set_status(s.ToString());
          }
        }
        break;
      case SingleOp::PUT:
        // batch_start_time_ = high_resolution_clock::now();
        // batch_counter_++;
        for(const auto& singleOp: op.ops()) {
          s = db_->Put(rocksdb::WriteOptions(), singleOp.key(), singleOp.value());
          if(!s.ok()){
            std::cout << "Put Failed : " << s.ToString() << std::endl;
            assert(false);
          }
          // std::cout << "Put ok\n";
          // return to replicator if tail
          if(db_options_->is_tail){
            singleOpReply = reply->add_replies();
            singleOpReply->set_id(singleOp.id());
            singleOpReply->set_type(SingleOpReply::PUT);
            singleOpReply->set_key(singleOp.key());
            if(s.ok()){
              // std::cout << "Put : (" << singleOp.key() /* << " ," << op.value() */ << ")\n"; 
              singleOpReply->set_ok(true);
            }else{
              std::cout << "Put Failed : " << s.ToString() << std::endl;
              singleOpReply->set_ok(false);
              singleOpReply->set_status(s.ToString());
            }
          }
        }
        // batch_end_time_ = high_resolution_clock::now();
        // std::cout << "thread " << map[std::this_thread::get_id()] << " processd batch " << batch_counter_.load() 
        //           << " in " << std::to_string(duration_cast<std::chrono::milliseconds>(batch_end_time_ - batch_start_time_).count())
        //           << " millisecs , size : " << op.ops_size() << std::endl;

        break;
      case SingleOp::DELETE:
        //TODO
        break;

      case SingleOp::UPDATE:
        // std::cout << "in UPDATE " << op.ops(0).key() << "\n"; 
        for(const auto& singleOp: op.ops()) {
          singleOpReply = reply->add_replies();
          singleOpReply->set_id(singleOp.id());
          s = db_->Get(rocksdb::ReadOptions(), singleOp.key(), &value);
          s = db_->Put(rocksdb::WriteOptions(), singleOp.key(), singleOp.value());
          singleOpReply->set_key(singleOp.key());
          singleOpReply->set_type(SingleOpReply::UPDATE);
          singleOpReply->set_status(s.ToString());
          if(s.ok()){
            singleOpReply->set_ok(true);
            singleOpReply->set_value(value);
          }else{
            singleOpReply->set_ok(false);
          }
        }
        break;

      default:
        std::cerr << "Unsupported Operation \n";
        break;
    }
}

// a streaming RPC used by the non-tail node to sync Version(view of sst files) states to the downstream node 
Status RubbleKvServiceImpl::Sync(ServerContext* context, 
              ServerReaderWriter<SyncReply, SyncRequest>* stream) {
    SyncRequest request;
    while (stream->Read(&request)) {
      SyncReply reply;
      HandleSyncRequest(&request, &reply);
      stream->Write(reply);
    }
    return Status::OK;
}

void RubbleKvServiceImpl::HandleSyncRequest(const SyncRequest* request, 
                          SyncReply* reply) {
      
  std::string args = request->args();
  reply->set_message(ApplyVersionEdits(args));
}

// Update the secondary's states by applying the version edits
// and ship sst to the downstream node if necessary
std::string RubbleKvServiceImpl::ApplyVersionEdits(const std::string& args) {
    log_apply_counter_++;
    std::cout << " --------[Secondary] Accepting Sync RPC " << log_apply_counter_.load() << " th times --------- \n";
    // example args json :
    // For a Flush:
    //{
    //     "BatchCount": 2,
    //     "IsFlush": true,
    //     "NextFileNum": 16,
    //     "EditList": [
    //         "{\"EditNumber\": 4, \"LogNumber\": 14, \"PrevLogNumber\": 0, \"BatchCount\": 2, \"AddedFiles\": [{\"Level\": 0, \"FileNumber\": 15, \"FileSize\": 64858, \"SmallestUserKey\": \"key00005281\", \"SmallestSeqno\": 5281, \"LargestUserKey\": \"key00007922\", \"LargestSeqno\": 7922}], \"ColumnFamily\": 0, \"IsFlush\": 1}"
    //     ]
    // }
    // For a compaction:
    //{
    //     "NextFileNum": 16,
    //     "EditList": [
    //         "{\"EditNumber\": 5, \"DeletedFiles\": [{\"Level\": 0, \"FileNumber\": 15}], \"AddedFiles\": [{\"Level\": 1, \"FileNumber\": 15, \"FileSize\": 64858, \"SmallestUserKey\": \"key00005281\", \"SmallestSeqno\": 5281, \"LargestUserKey\": \"key00007922\", \"LargestSeqno\": 7922}], \"ColumnFamily\": 0}"
    //     ]
    // }

    const json j_args = json::parse(args);
    // std::cout << j_args.dump(4) << std::endl;

    ResetStates();

    if(j_args.contains("IsFlush")){
      assert(j_args["IsFlush"].get<bool>());
      batch_count_ = j_args["BatchCount"].get<int>();
      is_flush_ = true;
    }
    if(j_args.contains("IsTrivial")){
      is_trivial_move_compaction_ = true;
    }

    request_id_ = j_args["Id"].get<uint64_t>();
    next_file_num_ = j_args["NextFileNum"].get<uint64_t>();
    rocksdb::autovector<rocksdb::VersionEdit*> edit_list;
    std::vector<rocksdb::VersionEdit> edits;
    for(const auto& edit_string : j_args["EditList"].get<std::vector<std::string>>()){  
      auto j_edit = json::parse(edit_string);
      edits.push_back(ParseJsonStringToVersionEdit(j_edit));      
    }
    for(auto& edit: edits) {
      edit_list.push_back(&edit);
    }

   // doesn't need to do anything for a trivial move compaction
    if(!is_trivial_move_compaction_){
      for(const auto& edit: edit_list){
        // this function is called by secondary nodes in the chain in rubble
        ios_ = UpdateSstViewAndShipSstFiles(*edit);
        assert(ios_.ok());
      }
      if(!is_tail_ && !piggyback_edits_){
        // non-tail node should also issue Sync rpc call to downstream node
        sync_client_->Sync(args);
        // std::cout << "[ Reply Status ]: " << sync_reply << std::endl;
      }
    }

    // logAndApply needs to hold the mutex
    rocksdb::InstrumentedMutexLock l(mu_);

    const rocksdb::MutableCFOptions* cf_options = default_cf_->GetCurrentMutableCFOptions();
    rocksdb::autovector<rocksdb::ColumnFamilyData*> cfds;
    cfds.emplace_back(default_cf_);

    rocksdb::autovector<const rocksdb::MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(cf_options);

    rocksdb::autovector<rocksdb::autovector<rocksdb::VersionEdit*>> edit_lists;
    edit_lists.emplace_back(edit_list);

    rocksdb::FSDirectory* db_directory = impl_->directories_.GetDbDir();
    rocksdb::MemTableList* imm = default_cf_->imm();
    
    // std::cout << "MemTable : " <<  json::parse(default_cf->mem()->DebugJson()).dump(4) << std::endl;
    // std::cout  << "Immutable MemTable list : " << json::parse(imm->DebugJson()).dump(4) << std::endl;
    // std::cout << " Current Version :\n " << default_cf_->current()->DebugString(false) <<std::endl;

    uint64_t current_next_file_num = version_set_->current_next_file_number();
    // set secondary version set's next file num according to the primary's next_file_num_
    version_set_->FetchAddFileNumber(next_file_num_ - current_next_file_num);
    assert(version_set_->current_next_file_number() == next_file_num_);

    // Calling LogAndApply on the secondary
    rocksdb::Status s = version_set_->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu_,
                      db_directory);
    if(s.ok()){
      std::cout << "[Secondary] logAndApply succeeds\n";
    }

    //Drop The corresponding MemTables in the Immutable MemTable List
    //If this version edit corresponds to a flush job
    if(is_flush_){
      assert(batch_count_ % num_of_added_files_ == 0);
      // creating a new verion after we applied the edit
      imm->InstallNewVersion();

      // All the later memtables that have the same filenum
      // are part of the same batch. They can be committed now.
      uint64_t mem_id = 1;  // how many memtables have been flushed.
      
      rocksdb::autovector<rocksdb::MemTable*> to_delete;
      // std::cout << s_.ToString() << std::endl;
      if(s.ok() && !default_cf_->IsDropped()){

        rocksdb::SuperVersion* sv = default_cf_->GetSuperVersion();
        rocksdb::MemTableListVersion* current = imm->current();
        // assert(imm->current()->GetMemlist().size() >= batch_count_) ? 
        // This is not always the case, sometimes secondary has only one immutable memtable in the list, say ID 89,
        // while the primary has 2 immutable memtables, say 89 and 90, with a more latest one,
        // so should set the number_of_immutable_memtable_to_delete to be the minimum of batch count and immutable memlist size
        int num_of_imm_to_delete = std::min(batch_count_, (int)current->GetMemlist().size());
        while(num_of_imm_to_delete -- > 0){

          rocksdb::MemTable* m = current->GetMemlist().back();
          m->SetFlushCompleted(true);
          m->SetFileNumber(next_file_num_ - num_of_imm_to_delete/(batch_count_/num_of_added_files_)); 

          // if (m->GetEdits().GetBlobFileAdditions().empty()) {
          //   ROCKS_LOG_BUFFER(log_buffer,
          //                   "[%s] Level-0 commit table #%" PRIu64
          //                   ": memtable #%" PRIu64 " done",
          //                   default_cf_->GetName().c_str(), m->file_number_, mem_id);
          // } else {
          //   ROCKS_LOG_BUFFER(log_buffer,
          //                   "[%s] Level-0 commit table #%" PRIu64
          //                   " (+%zu blob files)"
          //                   ": memtable #%" PRIu64 " done",
          //                   default_cf_->GetName().c_str(), m->file_number_,
          //                   m->edit_.GetBlobFileAdditions().size(), mem_id);
          // }

          assert(m->GetFileNumber() > 0);
          /* drop the corresponding immutable memtable in the list if version edit corresponds to a flush */
          // according the code comment in the MemTableList class : "The memtables are flushed to L0 as soon as possible and in any order." 
          // as far as I observe, it's always the back of the imm memlist gets flushed first, which is the earliest memtable
          // so here we always drop the memtable in the back of the list
          current->RemoveLast(sv->GetToDelete());

          imm->SetNumFlushNotStarted(current->GetMemlist().size());
          imm->UpdateCachedValuesFromMemTableListVersion();
          imm->ResetTrimHistoryNeeded();
          ++mem_id;
        }
      }else {
        //TODO : Commit Failed For Some reason, need to reset state
        std::cout << "[Secondary] Flush logAndApply Failed : " << s.ToString() << std::endl;
      }

      imm->SetCommitInProgress(false);
      // std::cout << " ----------- After RemoveLast : ( ImmutableList : " << json::parse(imm->DebugJson()).dump(4) << " ) ----------------\n";
      int size = static_cast<int> (imm->current()->GetMemlist().size());
      // std::cout << " memlist size : " << size << " , num_flush_not_started : " << imm->GetNumFlushNotStarted() << std::endl;
    }else { // It is either a trivial move compaction or a full compaction
      if(is_trivial_move_compaction_){
        if(!s.ok()){
          std::cout << "[Secondary] Trivial Move LogAndApply Failed : " << s.ToString() << std::endl;
        }
      }else{ // it's a full compaction
        std::cout << "[Secondary] Full compaction LogAndApply status : " << s.ToString() << std::endl;
      }
    }

    assert(s.ok());
    return s.ToString();
    // SetReplyMessage(reply, s);
}

  
void RubbleKvServiceImpl::ResetStates(){
    // reset those variables every time called Sync
    num_of_added_files_ = 0;
    is_flush_ = false;
    is_trivial_move_compaction_ = false;
    batch_count_ = 0;
    deleted_files_.clear();
    added_.clear();
}

// parse the version edit json string to rocksdb::VersionEdit 
rocksdb::VersionEdit RubbleKvServiceImpl::ParseJsonStringToVersionEdit(const json& j_edit /* json version edit */){
    rocksdb::VersionEdit edit;

    assert(j_edit.contains("AddedFiles"));
    if(j_edit.contains("IsFlush")){ // means edit corresponds to a flush job
        num_of_added_files_ += j_edit["AddedFiles"].get<std::vector<json>>().size();
        edit.MarkFlush();
    }

    if(j_edit.contains("LogNumber")){
        edit.SetLogNumber(j_edit["LogNumber"].get<uint64_t>());
    }
    if(j_edit.contains("PrevLogNumber")){
        edit.SetPrevLogNumber(j_edit["PrevLogNumber"].get<uint64_t>());
    }
    
    // right now, just use one column family(the default one)
    edit.SetColumnFamily(0);
    
    for(const auto& j_added_file : j_edit["AddedFiles"].get<std::vector<json>>()){
        // std::cout << j_added_file.dump(4) << std::endl;
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
        if(!is_trivial_move_compaction_){
          added_.emplace(file_num, j_added_file["Slot"].get<int>());
        }
        // max_file_num = std::max(max_file_num, (int)file_num);
        uint64_t file_size = j_added_file["FileSize"].get<uint64_t>();

        std::string file_checksum = j_added_file["FileChecksum"].get<std::string>();
        std::string file_checksum_func_name = j_added_file["FileChecksumFuncName"].get<std::string>();
        uint64_t file_creation_time = j_added_file["FileCreationTime"].get<uint64_t>();
        uint64_t oldest_ancester_time = j_added_file["OldestAncesterTime"].get<uint64_t>();

        const rocksdb::FileMetaData meta(file_num, 0/* path_id shoule be 0*/,
                                        file_size, 
                                        smallest, largest, 
                                        smallest_seqno, largest_seqno,
                                        false, 
                                        rocksdb::kInvalidBlobFileNumber,
                                        oldest_ancester_time,
                                        file_creation_time,
                                        file_checksum, 
                                        file_checksum_func_name);

        edit.AddFile(level, meta);
    }

    if(j_edit.contains("DeletedFiles")){
        for(const auto& j_delete_file : j_edit["DeletedFiles"].get<std::vector<json>>()){
            edit.DeleteFile(j_delete_file["Level"].get<int>(), j_delete_file["FileNumber"].get<uint64_t>());
            deleted_files_.push_back(j_delete_file["FileNumber"].get<uint64_t>());
        }   
    }
    return std::move(edit);
}

//called by secondary nodes to create a pool of preallocated ssts in rubble mode
rocksdb::IOStatus RubbleKvServiceImpl::CreateSstPool(){
    const std::string sst_dir = db_path_.path;
    uint64_t target_size = db_path_.target_size;
    // size_t write_buffer_size = cf_options_->write_buffer_size;
    uint64_t target_file_size_base = cf_options_->target_file_size_base;
    assert((target_file_size_base % (1 << 20)) == 0);
    //assume the target_file_size_base is an integer multiple of 1MB
    // use one more MB because of the footer, and pad to the buffer_size
    uint64_t buffer_size = target_file_size_base + (1 << 20);

    std::cout << "sst file size : " << buffer_size << std::endl;

    rocksdb::AlignedBuffer buf;
    rocksdb::IOStatus s;

    int preallocated_sst_pool_size = db_options_->preallocated_sst_pool_size;

    for (int i = 1; i <= preallocated_sst_pool_size; i++) {
        std::string sst_num = std::to_string(i);
        // rocksdb::WriteStringToFile(fs_, rocksdb::Slice(std::string(buffer_size, 'c')), sst_dir + "/" + fname, true);
        std::string sst_name = sst_dir + "/" + sst_num;
        s = fs_->FileExists(sst_name, rocksdb::IOOptions(), nullptr);
        if(!s.ok()) {
            std::unique_ptr<rocksdb::FSWritableFile> file;
            rocksdb::EnvOptions soptions;
            soptions.use_direct_writes = true;
            s = fs_->NewWritableFile(sst_name, soptions, &file, nullptr);
            if (!s.ok()) {
                return s;
            }
            if(i == 1){
                buf.Alignment(file->GetRequiredBufferAlignment());
                buf.AllocateNewBuffer(buffer_size);
                buf.PadWith(buffer_size, 'c');
            }

            s = file->Append(rocksdb::Slice(buf.BufferStart()), rocksdb::IOOptions(), nullptr);
            if (s.ok()) {
                s = file->Sync(rocksdb::IOOptions(), nullptr);
            }
            if (!s.ok()) {
                fs_->DeleteFile(sst_name, rocksdb::IOOptions(), nullptr);
                return s;
            }
        }
    }

    std::cout << "allocated " << db_options_->preallocated_sst_pool_size << " sst slots in " << sst_dir << std::endl;
    return s;
}


// In a 3-node setting, if it's the second node in the chain it should also ship sst files it received from the primary/first node
// to the tail/downstream node and also delete the ones that get deleted in the compaction
// for non-head node, should update sst bit map
// since second node's flush is disabled ,we should do the shipping here when it received Sync rpc call from the primary
/**
 * @param edit The version edit received from the priamry 
 * 
 */
rocksdb::IOStatus RubbleKvServiceImpl::UpdateSstViewAndShipSstFiles(const rocksdb::VersionEdit& edit){

    rocksdb::IOStatus ios;
    for(const auto& new_file: edit.GetNewFiles()){
        const rocksdb::FileMetaData& meta = new_file.second;
        uint64_t file_num = meta.fd.GetNumber();
        // int sst_real = TakeOneAvailableSstSlot(new_file.first, meta);
        int sst_real = added_[file_num];
        
        rocksdb::DirectReadKBytes(fs_, sst_real, 32, db_path_.path + "/");

        int len = std::to_string(file_num).length();
        std::string sst_file_name = std::string("000000").replace(6 - len, len, std::to_string(file_num)) + ".sst";
        std::string fname = rocksdb::TableFileName(ioptions_->cf_paths, file_num, meta.fd.GetPathId());
        // update secondary's view of sst files
        // std::cout << " Link " << sst_real << " to " << fname << std::endl;
        ios = fs_->LinkFile(db_path_.path + "/" + std::to_string(sst_real), fname, rocksdb::IOOptions(), nullptr);
        if(!ios.ok()){
            std::cout << ios.ToString() << std::endl;
            return ios;
        }else{
            std::cout << "[create new sst]: "  << sst_file_name << " , linking to " << sst_real << std::endl;
        }
        
        // tail node doesn't need to ship sst files
        if(!is_tail_){
            assert(db_options_->remote_sst_dir != "");
            std::string remote_sst_dir = db_options_->remote_sst_dir;
            if(remote_sst_dir[remote_sst_dir.length() - 1] != '/'){
                remote_sst_dir = remote_sst_dir + '/';
            }
            std::string remote_sst_fname = remote_sst_dir + std::to_string(sst_real);
            ios = rocksdb::CopySstFile(fs_, fname, remote_sst_fname, 0,  true);
            if (!ios.ok()){
                std::cerr << "[ File Ship Failed ] : " << meta.fd.GetNumber() << std::endl;
            }else {
                std::cout << "[ File Shipped ] : " << meta.fd.GetNumber() << std::endl;
            }
        }
    }
    
    // delete the ssts(the symbolic link) that get deleted in a non-trivial compaction
    for(const auto& delete_file : edit.GetDeletedFiles()){
        std::string file_number = std::to_string(delete_file.second);
        std::string sst_file_name = std::string("000000").replace(6 - file_number.length(), file_number.length(), file_number) + ".sst";
        std::string fname = db_path_.path + "/" + sst_file_name;
        ios = fs_->FileExists(fname, rocksdb::IOOptions(), nullptr);
        if (ios.ok()){
            // delete the symbolic link
            ios = fs_->DeleteFile(fname, rocksdb::IOOptions(), nullptr); 
            if(ios.IsIOError()){
                std::cerr << "[ File Deletion Failed ]:" <<  file_number << std::endl;
            }else if(ios.ok()){
                std::cout << "[ File Deletion] : " << sst_file_name << std::endl;
            // FreeSstSlot(delete_file.second);
            }
        }else {
            if (ios.IsNotFound()){
                std::cerr << "file :" << file_number << "does not exist \n";
            } else {
                std::cerr << "unknown error in sync_service_impl" << std::endl;
            }
        }
    }
    return ios;
}

// set the reply message according to the status
void RubbleKvServiceImpl::SetReplyMessage(SyncReply* reply,const rocksdb::Status& s){
    rocksdb::JSONWriter jw;

    json j_reply;
    j_reply["Id"] = request_id_;
    if(s.ok()){
        jw << "Status" << "Ok";
        
        if(!is_flush_ && !is_trivial_move_compaction_){
            jw << "Type" << "Full";
            jw << "Deleted";
            jw.StartArray();

            assert(!deleted_files_.empty());
            for (const auto& deleted_file : deleted_files_) {
                jw << deleted_file;
            }

            jw.EndArray();
        }else if(is_flush_){
            jw << "Type" << "Flush";
        }else{
            jw << "Type" << "Trivial";
        }
        jw.EndObject();

        std::string message = jw.Get();
        j_reply["Message"] = message;
        // std::cout << message << std::endl;
        reply->set_message(j_reply.dump());
    
    }else{
        jw << "Status" << "Failed";
        jw << "Reason" << s.ToString();
        jw.EndObject();

        std::string message = jw.Get();
        j_reply["Message"] = message;

        // std::cout << "Message : " << j_reply.dump(4) << std::endl;
        reply->set_message(j_reply.dump());
    }
}

    