#include "rubble_async_server.h"

static std::atomic<int> counter{0};
static std::unordered_map<std::thread::id, int> map;
static std::atomic<uint64_t> log_apply_counter_{0};
static std::atomic<uint64_t> version_edit_id_{0};
static std::multimap<uint64_t, rocksdb::VersionEdit> cached_edits_;

// async version of DoOp
void CallData::Proceed() {
  switch (status_) {
    case CallStatus::CREATE:
      status_ = CallStatus::PROCESS;
      service_->RequestDoOp(&ctx_, &request_, &responder_, cq_, cq_, (void*)this);
      db_options_ = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->db_options();
      break;
    case CallStatus::PROCESS:
      // Spawn a new CallData instance to serve new clients while we process
      // the one for this CallData. The instance will deallocate itself as
      // part of its FINISH state.
      new CallData(service_, cq_, db_, channel_);

      // The actual processing.
      HandleOp();
      reply_.set_batchsize(reply_.replies_size());
      counter.fetch_add(request_.ops_size());

      // if there is version edits piggybacked in the DoOp request, apply those edits
      if(request_.has_edits()){
        size_t size = request_.edits_size();
        rocksdb::InstrumentedMutexLock l(mu_);
        for(int i = 0; i < size; i++){
          ApplyVersionEdits(request_.edits(i));
        }
      }
      
      /* chain replication */
      // Forward the request to the downstream node in the chain if it's not a tail node
      if (!db_options_->is_tail) {
        // RUBBLE_LOG_INFO(logger_, "[Primary] thread %u Forwarded %lu ops\n", map[this_id], op_counter_.load());
        if(piggyback_edits_ && is_rubble_ && is_head_){
          std::vector<std::string> edits;
          edits_->GetEdits(edits);
          if(edits.size() != 0){
            size_t size = edits.size();
            // RUBBLE_LOG_INFO(logger_, "[primary] Got %u new version edits, op counter : %lu \n", static_cast<uint32_t>(size) ,op_counter_.load()); 
            request_.set_has_edits(true);
            for(const auto& edit : edits){
              // RUBBLE_LOG_INFO(logger_, "Added Version Edit : %s \n", edit.c_str());
              request_.add_edits(edit);
            }
            assert(request_.edits_size() == size);
          }else{
            request_.set_has_edits(false);
          }
        }
        if (forwarder_ == nullptr) {
          // std::cout << "init the forwarder" << "\n";
          forwarder_ = std::make_shared<Forwarder>(channel_);
        }
        forwarder_->Forward(request_);
        responder_.Finish(forwarder_->AsyncCompleteRpc(), Status::OK, this);
        // std::cout  << "counter : " << counter.load() << std::endl;
      } else {
        responder_.Finish(reply_, Status::OK, this);
        // std::cout  << "counter : " << counter.load() << std::endl;
      }
      status_ = CallStatus::FINISH;
      break;
    case CallStatus::FINISH:
      delete this;
      break;
    default:
      std::cerr << "Should not reach here\n";
      assert(false);
  }
}


void CallData::HandleOp(){
    std::string value;
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
    assert(request_.ops_size() > 0);

    SingleOpReply* reply;
    reply_.clear_replies();
    // std::cout << "thread: " << map[std::this_thread::get_id()] << " op_counter: " << op_counter_ << std::endl;
    //     <<  " first key in batch: " << request_.ops(0).key() << " size: " << request_.ops_size() << "\n";
    switch (request_.ops(0).type())
    {
      case rubble::GET:
        for(const auto& request: request_.ops()) {
          reply = reply_.add_replies();
          reply->set_id(request.id());
          s_ = db_->Get(rocksdb::ReadOptions(), request.key(), &value);
          reply->set_key(request.key());
          reply->set_type(rubble::GET);
          reply->set_status(s_.ToString());
          if(s_.ok()){
            reply->set_ok(true);
            reply->set_value(value);
          }else{
            reply->set_ok(false);
            reply->set_status(s_.ToString());
          }
        }
        break;
      case rubble::PUT:
        // batch_start_time_ = high_resolution_clock::now();
        for(const auto& request: request_.ops()) {
          s_ = db_->Put(rocksdb::WriteOptions(), request.key(), request.value());
         if(!s_.ok()){
	        std::cout << "Put Failed : " << s_.ToString() << std::endl;
	      }
          // std::cout << "Put ok\n";
          // return to replicator if tail
          if(db_options_->is_tail){
            reply = reply_.add_replies();
            reply->set_id(request.id());
            reply->set_type(rubble::PUT);
            reply->set_key(request.key());
            if(s_.ok()){
              // std::cout << "Put : (" << request.key() /* << " ," << request_.value() */ << ")\n"; 
              reply->set_ok(true);
            }else{
              std::cout << "Put Failed : " << s_.ToString() << std::endl;
              reply->set_ok(false);
              reply->set_status(s_.ToString());
            }
          }
        }
        // batch_end_time_ = high_resolution_clock::now();
        // std::cout << "thread " << map[std::this_thread::get_id()] << " processd batch " << batch_counter_.load() 
        //           << " in " << std::to_string(duration_cast<std::chrono::milliseconds>(batch_end_time_ - batch_start_time_).count())
        //           << " millisecs , size : " << request_.ops_size() << std::endl;

        break;
      case rubble::DELETE:
        //TODO
        break;

      case rubble::UPDATE:
        // std::cout << "in UPDATE " << request_.ops(0).key() << "\n"; 
        for(const auto& request: request_.ops()) {
          reply = reply_.add_replies();
          reply->set_id(request.id());
          s_ = db_->Get(rocksdb::ReadOptions(), request.key(), &value);
          s_ = db_->Put(rocksdb::WriteOptions(), request.key(), request.value());
          reply->set_key(request.key());
          reply->set_type(rubble::UPDATE);
          reply->set_status(s_.ToString());
          if(s_.ok()){
            reply->set_ok(true);
            reply->set_value(value);
          }else{
            reply->set_ok(false);
          }
        }
        break;

      default:
        std::cerr << "Unsupported Operation \n";
        break;
    }
}

// Update the secondary's states by applying the version edits
// and ship sst to the downstream node if necessary
std::string CallData::ApplyVersionEdits(const std::string& args) {
    const json j_args = json::parse(args);
    bool is_flush = false;
    bool is_trivial_move = false;
    int batch_count = 0;

    if(j_args.contains("IsFlush")){
      assert(j_args["IsFlush"].get<bool>());
      batch_count = j_args["BatchCount"].get<int>();
      is_flush = true;
    }

    if(j_args.contains("IsTrivial")){
      is_trivial_move = true;
    }

    uint64_t version_edit_id = j_args["Id"].get<uint64_t>();
    uint64_t expected = version_edit_id_.load() + 1;
    
    uint64_t next_file_num = j_args["NextFileNum"].get<uint64_t>();
    rocksdb::autovector<rocksdb::VersionEdit*> edit_list;
    std::vector<rocksdb::VersionEdit> edits;
    for(const auto& edit_string : j_args["EditList"].get<std::vector<std::string>>()){
      // std::cout << edit_string << std::endl;  
      auto j_edit = json::parse(edit_string);
      auto edit = ParseJsonStringToVersionEdit(j_edit);
      // // RUBBLE_LOG_INFO(logger_, "Got a new edit %s ", edit.DebugString().c_str());
      if(is_flush){
        edit.SetBatchCount(batch_count);
      }else if(is_trivial_move){
        edit.MarkTrivialMove();
      }
      edit.SetNextFile(next_file_num);
      edits.push_back(edit);      
    }
  
    if(version_edit_id != expected){
      // RUBBLE_LOG_INFO(logger_, "Version Edit arrives out of order, expecting %lu, cache %lu \n", expected, version_edit_id);
      for(const auto& edit : edits){
       cached_edits_.insert({edit.GetEditNumber(), edit});
      }
      return "ok";
    }

    version_edit_id_.store(version_edit_id);
    ApplyOneVersionEdit(edits);
    edits.clear();
    if(cached_edits_.size() != 0){
      expected = version_edit_id + 1;
      int count = cached_edits_.count(expected);
      // auto it = cached_edits_.cbegin();
      while(count != 0){
        // RUBBLE_LOG_INFO(logger_, "Got %d edits in cache, edit id : %lu \n", count, expected);
        for(int i = 0; i < count; i++){
          auto it = cached_edits_.cbegin();
          assert(it->first == expected);
          edits.push_back(it->second);
          cached_edits_.erase(it);
        }
        version_edit_id_.store(expected);
        ApplyOneVersionEdit(edits);
        edits.clear();
        expected++;
        count = cached_edits_.count(expected);
      }
    }

    return "ok";
}


std::string CallData::ApplyOneVersionEdit(std::vector<rocksdb::VersionEdit>& edits) {
   
    bool is_flush = false;
    bool is_trivial_move = false;
    int batch_count = 0;
    if(edits.size() == 2){
      for(const auto& edit : edits){
        assert(edit.IsFlush());
      }
    }
    auto& first_edit = edits[0];
    if(first_edit.IsFlush()){
      is_flush = true;
    }else if(first_edit.IsTrivialMove()){
      is_trivial_move = true;
    }

    batch_count += first_edit.GetBatchCount();
    uint64_t next_file_num = first_edit.GetNextFile();

    rocksdb::IOStatus ios;
    for(const auto& edit: edits){
      ios = UpdateSstViewAndShipSstFiles(edit);
      assert(ios.ok());
    }
     
    rocksdb::autovector<rocksdb::VersionEdit*> edit_list;
    for(auto& edit : edits){  
      edit_list.push_back(&edit);
    }

    const rocksdb::MutableCFOptions* cf_options = default_cf_->GetCurrentMutableCFOptions();
    rocksdb::autovector<rocksdb::ColumnFamilyData*> cfds;
    cfds.emplace_back(default_cf_);

    rocksdb::autovector<const rocksdb::MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(cf_options);

    rocksdb::autovector<rocksdb::autovector<rocksdb::VersionEdit*>> edit_lists;
    edit_lists.emplace_back(edit_list);

    rocksdb::FSDirectory* db_directory = impl_->directories_.GetDbDir();
    rocksdb::MemTableList* imm = default_cf_->imm();
    
    uint64_t current_next_file_num = version_set_->current_next_file_number();
    // set secondary version set's next file num according to the primary's next_file_num_
    version_set_->FetchAddFileNumber(next_file_num - current_next_file_num);
    assert(version_set_->current_next_file_number() == next_file_num);

    // log_apply_counter_.fetch_add(1);
    // RUBBLE_LOG_INFO(logger_, " Accepting Sync %lu th times \n", log_apply_counter_.load());
    // Calling LogAndApply on the secondary
    rocksdb::Status s = version_set_->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu_,
                      db_directory);
    if(s.ok()){
      // RUBBLE_LOG_INFO(logger_, "[Secondary] logAndApply succeeds \n");
    }else{
      // RUBBLE_LOG_ERROR(logger_, "[Secondary] logAndApply failed : %s \n" , s.ToString().c_str());
      std::cerr << "[Secondary] logAndApply failed : " << s.ToString() << std::endl;
    }

    //Drop The corresponding MemTables in the Immutable MemTable List
    //If this version edit corresponds to a flush job
    if(is_flush){
      // creating a new verion after we applied the edit
      imm->InstallNewVersion();

      // All the later memtables that have the same filenum
      // are part of the same batch. They can be committed now.
      uint64_t mem_id = 1;  // how many memtables have been flushed.
      
      rocksdb::autovector<rocksdb::MemTable*> to_delete;
      if(s.ok() && !default_cf_->IsDropped()){

        rocksdb::SuperVersion* sv = default_cf_->GetSuperVersion();
        rocksdb::MemTableListVersion* current = imm->current();
        // assert(imm->current()->GetMemlist().size() >= batch_count_) ? 
        // This is not always the case, sometimes secondary has only one immutable memtable in the list, say ID 89,
        // while the primary has 2 immutable memtables, say 89 and 90, with a more latest one,
        // so should set the number_of_immutable_memtable_to_delete to be the minimum of batch count and immutable memlist size
        int imm_size = (int)current->GetMemlist().size();
        int num_of_imm_to_delete = std::min(batch_count, imm_size);
        // RUBBLE_LOG_INFO(logger_ , "memlist size : %d, batch count : %d \n", imm_size ,  batch_count);
        // fprintf(stdout, "memlist size : %d, bacth count : %d ,num_of_imms_to_delete : %d \n", imm_size ,batch_count, num_of_imm_to_delete);
        int i = 0;
        while(num_of_imm_to_delete -- > 0){
          rocksdb::MemTable* m = current->GetMemlist().back();
          m->SetFlushCompleted(true);

          auto& edit = edit_list[i];
          auto& new_files = edit->GetNewFiles();
          m->SetFileNumber(new_files[0].second.fd.GetNumber()); 
          if(edit->GetBatchCount() == 1){
            i++;
          }
          
          // RUBBLE_LOG_INFO(logger_,
                        // "[%s] Level-0 commit table #%lu : memtable #%lu done",
                        // default_cf_->GetName().c_str(), m->GetFileNumber(), mem_id);

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
      // RUBBLE_LOG_INFO(logger_, "After RemoveLast : ( ImmutableList size : %u ) \n", static_cast<uint32_t>(imm->current()->GetMemlist().size()));
      // // RUBBLE_LOG_INFO(logger_, "After RemoveLast : ( ImmutableList : %s ) \n", json::parse(imm->DebugJson()).dump(4).c_str());
      int size = static_cast<int> (imm->current()->GetMemlist().size());
      // std::cout << " memlist size : " << size << " , num_flush_not_started : " << imm->GetNumFlushNotStarted() << std::endl;
    }else { // It is either a trivial move compaction or a full compaction
      if(is_trivial_move){
        if(!s.ok()){
          std::cout << "[Secondary] Trivial Move LogAndApply Failed : " << s.ToString() << std::endl;
        }else{
          // std::cout << "[Secondary] Trivial Move LogAndApply Succeeds \n";
        }
      }else{ // it's a full compaction
        // std::cout << "[Secondary] Full compaction LogAndApply status : " << s.ToString() << std::endl;
      }
    }

    assert(s.ok());
    std::string ret = "ok";
    return ret;
    // SetReplyMessage(reply, s, is_flush, is_trivial_move);
}


// parse the version edit json string to rocksdb::VersionEdit 
rocksdb::VersionEdit CallData::ParseJsonStringToVersionEdit(const json& j_edit /* json version edit */){
    rocksdb::VersionEdit edit;

    assert(j_edit.contains("AddedFiles"));
    if(j_edit.contains("IsFlush")){ // means edit corresponds to a flush job
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

    assert(j_edit.contains("EditNumber"));
    edit.SetEditNumber(j_edit["EditNumber"].get<uint64_t>());
    
    for(const auto& j_added_file : j_edit["AddedFiles"].get<std::vector<json>>()){
        // std::cout << j_added_file.dump(4) << std::endl;
        assert(!j_added_file["SmallestUserKey"].is_null());
        assert(!j_added_file["SmallestSeqno"].is_null());
        // TODO: should decide ValueType according to the info in the received version edit 
        // basically pass the ValueType of the primary's version Edit's smallest/largest InterKey's ValueType
        rocksdb::InternalKey smallest(rocksdb::Slice(j_added_file["SmallestUserKey"].get<std::string>()), 
                                      j_added_file["SmallestSeqno"].get<uint64_t>(),
                                      rocksdb::ValueType::kTypeValue);

        assert(smallest.Valid());

        uint64_t smallest_seqno = j_added_file["SmallestSeqno"].get<uint64_t>();
        
        assert(!j_added_file["LargestUserKey"].is_null());
        assert(!j_added_file["LargestSeqno"].is_null());
        rocksdb::InternalKey largest(rocksdb::Slice(j_added_file["LargestUserKey"].get<std::string>()), 
                                      j_added_file["LargestSeqno"].get<uint64_t>(),
                                      rocksdb::ValueType::kTypeValue);

        assert(largest.Valid());

        uint64_t largest_seqno = j_added_file["LargestSeqno"].get<uint64_t>();
        int level = j_added_file["Level"].get<int>();
        uint64_t file_num = j_added_file["FileNumber"].get<uint64_t>();

        if(j_added_file.contains("Slot")){
          edit.TrackSlot(file_num, j_added_file["Slot"].get<int>());
        }
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
        }   
    }
    return std::move(edit);
}
  
// In a 3-node setting, if it's the second node in the chain it should also ship sst files it received from the primary/first node
// to the tail/downstream node and also delete the ones that get deleted in the compaction
// for non-head node, should update sst bit map
// since second node's flush is disabled ,we should do the shipping here when it received Sync rpc call from the primary
/**
 * @param edit The version edit received from the priamry 
 * 
 */
rocksdb::IOStatus CallData::UpdateSstViewAndShipSstFiles(const rocksdb::VersionEdit& edit){
    if(edit.IsTrivialMove()){
      return rocksdb::IOStatus::OK();
    }
    rocksdb::IOStatus ios;
    for(const auto& new_file: edit.GetNewFiles()){
        const rocksdb::FileMetaData& meta = new_file.second;
        uint64_t file_num = meta.fd.GetNumber();
        // int sst_real = TakeOneAvailableSstSlot(new_file.first, meta);
        int sst_real = edit.GetSlot(file_num);
        
        // rocksdb::DirectReadKBytes(fs_, sst_real, 32, db_path_.path + "/");

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
          // RUBBLE_LOG_INFO(logger_, "[create new sst]: %s , linking to %d \n", sst_file_name.c_str(), sst_real);
          // std::cout << "[create new sst]: "  << sst_file_name << " , linking to " << sst_real << std::endl;
        }
        
        // tail node doesn't need to ship sst files
        if(!is_tail_){
            assert(db_options_->remote_sst_dir != "");
            std::string remote_sst_dir = db_options_->remote_sst_dir;
            if(remote_sst_dir[remote_sst_dir.length() - 1] != '/'){
                remote_sst_dir = remote_sst_dir + '/';
            }
            std::string remote_sst_fname = remote_sst_dir + std::to_string(sst_real);
            // maybe not ship the sst file here, instead ship after we finish the logAndApply..
            ios = rocksdb::CopySstFile(fs_, fname, remote_sst_fname, 0,  true);
            if (!ios.ok()){
                std::cerr << "[ File Ship Failed ] : " << meta.fd.GetNumber() << std::endl;
            }else {
                std::cout << "[ File Shipped ] : " << meta.fd.GetNumber() << std::endl;
            }
        }
    }
    
    // TODO: should delete file after logAndApply finishes and invalidated the block cache for the deleted files
    // delete the ssts(the hard link) that get deleted in a non-trivial compaction
    for(const auto& delete_file : edit.GetDeletedFiles()){
        uint64_t file_number = delete_file.second;
        std::string file_path = rocksdb::MakeTableFileName(ioptions_->cf_paths[0].path, file_number);

        std::string sst_file_name = file_path.substr((file_path.find_last_of("/") + 1));
        // std::string fname = db_path_.path + "/" + sst_file_name;
        ios = fs_->FileExists(file_path, rocksdb::IOOptions(), nullptr);
        if (ios.ok()){
            // delete the symbolic link
            ios = fs_->DeleteFile(file_path, rocksdb::IOOptions(), nullptr); 
            if(ios.IsIOError()){
                std::cerr << "[ File Deletion Failed ]:" <<  file_path << std::endl;
            }else if(ios.ok()){
              // RUBBLE_LOG_INFO(logger_,"[File Deletion ]: %s \n", sst_file_name.c_str());
              // std::cout << "[ File Deletion] : " << file_path << std::endl;
              // FreeSstSlot(delete_file.second);
            }
        }else {
            if (ios.IsNotFound()){
                std::cerr << "file :" << file_path << "does not exist \n";
            } else {
                std::cerr << "unknown error in sync_service_impl" << std::endl;
            }
        }
    }
    return ios;
}

ServerImpl::ServerImpl(const std::string& server_addr, rocksdb::DB* db, RubbleKvStoreService::AsyncService* service)
   :server_addr_(server_addr), db_(db), service_(service) {
        auto db_options = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->db_options(); 
        std::cout << "target address : " << db_options->target_address << std::endl;
        if(db_options->target_address != ""){
        channel_ = grpc::CreateChannel(db_options->target_address, grpc::InsecureChannelCredentials());
        assert(channel_ != nullptr);
        if(db_options->is_rubble && !db_options->is_primary) {
          std::cout << "init sync_service --- is_rubble: " << db_options->is_rubble << "; is_head: " << db_options->is_primary << std::endl;
          rocksdb::IOStatus ios_ = CreateSstPool();
          if(!ios_.ok()){
            std::cout << "allocate sst pool failed :" << ios_.ToString() << std::endl;
            assert(false);
          }
          std::cout << "[secondary] sst pool allocation finished" << std::endl;
        }
    }
}

ServerImpl::~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (const auto& _cq : m_cq)
        _cq->Shutdown();
}

// There is no shutdown handling in this code.
void ServerImpl::Run(int g_thread_num, int g_pool, int g_cq_num) {

    builder_.AddListeningPort(server_addr_, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an asynchronous DoOp service and synchronous Sync service
    builder_.RegisterService(service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
   
    for (int i = 0; i < g_cq_num; ++i) {
        //cq_ = builder.AddCompletionQueue();
        m_cq.emplace_back(builder_.AddCompletionQueue());
    }

    // Finally assemble the server.
    server_ = builder_.BuildAndStart();
  
    // Proceed to the server's main loop.
    std::vector<std::thread*> _vec_threads;

    for (int i = 0; i < g_thread_num; ++i) {
        int _cq_idx = i % g_cq_num;
        for (int j = 0; j < g_pool; ++j) {
            new CallData(service_, m_cq[_cq_idx].get(), db_, channel_);
        }
        auto new_thread =  new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx);
        map[new_thread->get_id()] = i;
        _vec_threads.emplace_back(new_thread);
    }

    std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

    for (const auto& _t : _vec_threads)
        _t->join();
}


// This can be run in multiple threads if needed.
void  ServerImpl::HandleRpcs(int cq_idx) {
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
      GPR_ASSERT(ok);

      CallData* _p_ins = (CallData*)tag;
      _p_ins->Proceed();
    }
}

rocksdb::IOStatus ServerImpl::CreateSstPool() {
    auto db_options_ = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->db_options();
    auto fs_ = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->GetColumnFamilySet()->GetDefault()->ioptions()->fs;
    auto cf_options_ = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->GetColumnFamilySet()->GetDefault()->GetCurrentMutableCFOptions();
    rocksdb::DbPath db_path_ = db_options_->db_paths.front();
    const std::string sst_dir = db_path_.path;
    uint64_t target_size = db_path_.target_size;
    // size_t write_buffer_size = cf_options_->write_buffer_size;
    uint64_t target_file_size_base = cf_options_->target_file_size_base;
    assert((target_file_size_base % (1 << 20)) == 0);
    //assume the target_file_size_base is an integer multiple of 1MB
    // use one more MB because of the footer, and pad to the buffer_size
    uint64_t buffer_size = target_file_size_base + db_options_->sst_pad_len;
    uint64_t big_buffer_size = 2*target_file_size_base + db_options_->sst_pad_len;

    std::cout << "sst file size : " << buffer_size << std::endl;

    rocksdb::AlignedBuffer buf;
    rocksdb::IOStatus s;
    
    int max_num_mems_to_flush = db_options_->max_num_mems_in_flush;
    int pool_size = db_options_->preallocated_sst_pool_size;
    int big_sst_pool_size = 10;
    for (int i = 1; i <= pool_size + (max_num_mems_to_flush - 1)* big_sst_pool_size; i++) {
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
            }else if( i > pool_size && ( ((i - pool_size) % big_sst_pool_size) == 1) ){
                int times = (i - pool_size) / big_sst_pool_size + 2;
                std::cout << " times : " << times << std::endl;
                buffer_size = times * target_file_size_base + db_options_->sst_pad_len;
                buf.Alignment(file->GetRequiredBufferAlignment());
                buf.AllocateNewBuffer(buffer_size);
                buf.PadWith(buffer_size - buf.CurrentSize(), 'c');
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

void RunAsyncServer(rocksdb::DB* db, const std::string& server_addr){
  
  RubbleKvStoreService::AsyncService service;

  int g_thread_num = 16;
  int g_cq_num = 16;
  int g_pool = 1;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // ServerBuilder builder;
  // builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
  std::cout << "Server listening on " << server_addr << std::endl;
  // builder.RegisterService(&service);
  ServerImpl server_impl(server_addr, db, &service);
  server_impl.Run(g_thread_num, g_pool, g_cq_num);
}

