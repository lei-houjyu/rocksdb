#pragma once

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <string> 
#include <memory>
#include <nlohmann/json.hpp>
#include <unordered_map>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/alarm.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"

#include "rocksdb/db.h"
#include "port/port_posix.h"
#include "port/port.h"
#include "db/version_edit.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "util/aligned_buffer.h"
#include "file/read_write_util.h"

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

using json = nlohmann::json;

//implements the Sync rpc call 
class SyncServiceImpl final : public  RubbleKvStoreService::WithAsyncMethod_DoOp<RubbleKvStoreService::Service> {
  public:
    explicit SyncServiceImpl(rocksdb::DB* db)
      :db_(db), impl_((rocksdb::DBImpl*)db_), 
       mu_(impl_->mutex()),
       version_set_(impl_->TEST_GetVersionSet()),
       db_options_(version_set_->db_options()),
       is_rubble_(db_options_->is_rubble),
       is_head_(db_options_->is_primary),
       is_tail_(db_options_->is_tail),
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
    };

    ~SyncServiceImpl() {
      delete db_;
    }
  
    //an Unary RPC call used by the non-tail node to sync Version(view of sst files) states to the downstream node 
  Status Sync(ServerContext* context, const SyncRequest* request, 
                          SyncReply* reply) override {
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

    std::string args = request->args();
    const json j_args = json::parse(args);
    // std::cout << j_args.dump(4) << std::endl;

    // reset those variables every time called Sync
    num_of_added_files_ = 0;
    is_flush_ = false;
    is_trivial_move_compaction_ = false;
    batch_count_ = 0;

    if(j_args.contains("IsFlush")){
      assert(j_args["IsFlush"].get<bool>());
      batch_count_ = j_args["BatchCount"].get<int>();
      is_flush_ = true;
    }
    if(j_args.contains("IsTrivial")){
      is_trivial_move_compaction_ = true;
    }

    next_file_num_ = j_args["NextFileNum"].get<uint64_t>();
    rocksdb::autovector<rocksdb::VersionEdit*> edit_list;
    rocksdb::VersionEdit edit;
    for(const auto& edit_string : j_args["EditList"].get<std::vector<string>>()){ 
      // std::cout << edit_string.dump() <<std::endl;  
      auto j_edit = json::parse(edit_string);
      edit = ParseJsonStringToVersionEdit(j_edit);
      edit_list.push_back(&edit);
    }

   // doesn't need to do anything for a trivial move compaction
    if(!is_trivial_move_compaction_){
      // called by secondary node
      for(const auto& edit: edit_list){
        // this function is called by secondary nodes in the chain in rubble
        ios_ = UpdateSstBitMapAndShipSstFiles(*edit);
        assert(ios_.ok());
      }
      if(!is_tail_){
        // non-tail node should also issue Sync rpc call to downstream node
        std::string sync_reply = sync_client_->Sync(*request);
        std::cout << "[ Reply Status ]: " << sync_reply << std::endl;
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
    s_ = version_set_->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu_,
                      db_directory);
    if(s_.ok()){
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
      if(s_.ok() && !default_cf_->IsDropped()){

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
        std::cout << "[Secondary] Flush logAndApply Failed : " << s_.ToString() << std::endl;
      }

      imm->SetCommitInProgress(false);
      // std::cout << " ----------- After RemoveLast : ( ImmutableList : " << json::parse(imm->DebugJson()).dump(4) << " ) ----------------\n";
      int size = static_cast<int> (imm->current()->GetMemlist().size());
      // std::cout << " memlist size : " << size << " , num_flush_not_started : " << imm->GetNumFlushNotStarted() << std::endl;
    }else { // It is either a trivial move compaction or a full compaction
      if(is_trivial_move_compaction_){
        if(!s_.ok()){
          std::cout << "[Secondary] Trivial Move LogAndApply Failed : " << s_.ToString() << std::endl;
        }
      }else{ // it's a full compaction
        std::cout << " [Secondary] Full compaction LogAndApply status :" << s_.ToString() << std::endl;
      }
    }

    if(s_.ok()){
      reply->set_message("Succeeds");
    }else{
      std::string failed = "Failed : " + s_.ToString();
      reply->set_message(failed);
    }
    // rocksdb::VersionStorageInfo::LevelSummaryStorage tmp;
    // auto vstorage = default_cf_->current()->storage_info();
    // const char* c = vstorage->LevelSummary(&tmp);
    // std::cout << " VersionStorageInfo->LevelSummary : " << std::string(c) << std::endl;
    return Status::OK;
  }

  private:
    //called by secondary nodes to create a pool of preallocated ssts in rubble mode
    rocksdb::IOStatus CreateSstPool(){
      const std::string sst_dir = db_path_.path;
      uint64_t target_size = db_path_.target_size;
      // size_t write_buffer_size = cf_options_->write_buffer_size;
      uint64_t target_file_size_base = (((cf_options_->target_file_size_base >> 20) + 1) << 20);
      //assume the write buffer size is an integer multiple of 1MB
      // use one more MB because of the footer, and pad to the buffer_size
      // assert((write_buffer_size % (1 << 20)) == 0);
      // uint64_t buffer_size = ( write_buffer_size + ( 1 << 20));
      uint64_t buffer_size = target_file_size_base;
    
      std::cout << "sst file size : " << buffer_size << std::endl;

      rocksdb::AlignedBuffer buf;
      rocksdb::IOStatus s;
      for (int i = 1; i <= db_options_->preallocated_sst_pool_size; i++) {
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

      // number of big sst file(which is 4 or 5 times as large as the normal sst file) to allocate
      int big_sst_num = db_options_->preallocated_sst_pool_size / 20;
      big_sst_num_ = big_sst_num;
      for(int i = 1 ; i <=  big_sst_num; i++){
        std::string sst_num = std::to_string(db_options_->preallocated_sst_pool_size + i);
        rocksdb::WriteStringToFile(fs_, rocksdb::Slice(std::string(buffer_size * 4, 'c')), sst_dir + "/" + sst_num,true);

        sst_num = std::to_string(db_options_->preallocated_sst_pool_size + i + big_sst_num);
        rocksdb::WriteStringToFile(fs_, rocksdb::Slice(std::string(buffer_size * 5, 'c')), sst_dir + "/" + sst_num,true);
      }

      std::cout << "allocated " << db_options_->preallocated_sst_pool_size << " sst slots in " << sst_dir << std::endl;
      return s;
    }

   // parse the version edit json string to rocksdb::VersionEdit 
   rocksdb::VersionEdit ParseJsonStringToVersionEdit(const json& j_edit /* json version edit */){
     rocksdb::VersionEdit edit;
      // std::cout << "Dumped VersionEdit : " << j_edit.dump(4) << std::endl;
      // Dumped VersionEdit : {
      //     "AddedFiles": [
      //         {
      //             "FileNumber": 12,
      //             "FileSize": 64547,
      //             "LargestSeqno": 5272,
      //             "LargestUserKey": "key00005272",
      //             "Level": 0,
      //             "SmallestSeqno": 2644,
      //             "SmallestUserKey": "key00002644"
      //         }
      //     ],
      //     "BatchCount": 2,
      //     "EditNumber": 2,
      //     "IsFlush": 1,
      //     "LogNumber": 11,
      //     "PrevLogNumber": 0
      // }

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
      // assert(!j_edit["ColumnFamily"].is_null());
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
          // max_file_num = std::max(max_file_num, (int)file_num);
          uint64_t file_size = j_added_file["FileSize"].get<uint64_t>();
    
          const rocksdb::FileMetaData meta(file_num, 0/* path_id shoule be 0*/,
                                          file_size, 
                                          smallest, largest, 
                                          smallest_seqno, largest_seqno,
                                          false, 
                                          rocksdb::kInvalidBlobFileNumber,
                                          rocksdb::kUnknownOldestAncesterTime,
                                          rocksdb::kUnknownFileCreationTime,
                                          rocksdb::kUnknownFileChecksum, 
                                          rocksdb::kUnknownFileChecksumFuncName);

          edit.AddFile(level, meta);
      }

      if(j_edit.contains("DeletedFiles")){
        for(const auto& j_delete_file : j_edit["DeletedFiles"].get<std::vector<json>>()){
          edit.DeleteFile(j_delete_file["Level"].get<int>(), j_delete_file["FileNumber"].get<uint64_t>());
        }
      }
      return std::move(edit);
    }


    // for a new added file, take a sst slot
    int TakeOneAvailableSstSlot(int level,const rocksdb::FileMetaData& meta){
      int sst_real = 0;
      // auto& meta = new_file.second;
      uint64_t file_number  = meta.fd.GetNumber();
      int start, end;
      if(IsL0CompactionOutputSst(level, meta)){
        int times = (meta.fd.GetFileSize() >> 20 )/(cf_options_->target_file_size_base >> 20);
        start = db_options_->preallocated_sst_pool_size + 1 + (times == 4 ? 0 : big_sst_num_);
        end = start + big_sst_num_ - 1;
      }else{
        start = 1;
        end = db_options_->preallocated_sst_pool_size;
      }
      
      for(int i = start; i <= end; i++){
        if(sst_bit_map_.find(i) == sst_bit_map_.end()){
          // if not found, means slot is not occupied
          sst_real = i;
          break;
        }
      }
      assert(sst_real != 0);
      sst_bit_map_.emplace(sst_real,file_number);
      return sst_real;
    }

    // if a file gets deleted, free its slot
    void FreeSstSlot(uint64_t file_num){
      auto it = sst_bit_map_.begin();
      for(; it != sst_bit_map_.end(); ++it){
        if(it->second == file_num){
          std::cout << "[ File Deleted ] : " <<  std::to_string(file_num) << ", free slot : " << it->first << std::endl;
          sst_bit_map_.erase(it);
          break;
        }
      }
      assert(it != sst_bit_map_.end());
    }

    // normally new sst file's size will be around target_file_size_base
    // but a L0 compaction will output file whose size is about 4 or 5 times large as normal sst file
    // check if the new sst file is Level 0 compaction 
    bool IsL0CompactionOutputSst(int level,const rocksdb::FileMetaData& meta){
      // auto& meta = new_file.second;
      int times = (meta.fd.GetFileSize() >> 20)/ (cf_options_->target_file_size_base >> 20);
      if(times >= 2){
        assert(level == 0);
        assert(times == 4 || times == 5);
        return true;
      }else{
        return false;
      }
    }

    // In a 3-node setting, if it's the second node in the chain it should also ship sst files it received from the primary/first node
    // to the tail/downstream node and also delete the ones that get deleted in the compaction
    // for non-head node, should update sst bit map
    // since second node's flush is disabled ,we should do the shipping here when it received Sync rpc call from the primary
    /**
     * @param edit The version edit received from the priamry 
     * 
     */
    rocksdb::IOStatus UpdateSstBitMapAndShipSstFiles(const rocksdb::VersionEdit& edit){

      rocksdb::IOStatus ios;
      for(const auto& new_file: edit.GetNewFiles()){
        const rocksdb::FileMetaData& meta = new_file.second;

        int sst_real = TakeOneAvailableSstSlot(new_file.first, new_file.second);
        if(sst_real > db_options_->preallocated_sst_pool_size){
          // assert(!edit.IsFlush());
          assert(new_file.first == 0);
        }
        
        // rocksdb::DirectReadKBytes(fs_, sst_real, 32, db_path_.path + "/");
        std::string fname = rocksdb::TableFileName(ioptions_->cf_paths,
                        meta.fd.GetNumber(), meta.fd.GetPathId());
        // update secondary's view of sst files
        std::cout << " Link " << sst_real << " to " << fname << std::endl;
        ios = fs_->LinkFile(db_path_.path + "/" + std::to_string(sst_real), fname, rocksdb::IOOptions(), nullptr);
        if(!ios.ok()){
          std::cout << ios.ToString() << std::endl;
          return ios;
        }else{
          std::cout << "[create new sst]: "  << fname << " , linking to " << sst_real << std::endl;
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
            FreeSstSlot(delete_file.second);
          }
        }else {
          if (ios.IsNotFound()){
            std::cerr << "file :" << file_number << "does not exist \n";
          }
        }
      }
      return ios;
    }

    // db instance
    rocksdb::DB* db_ = nullptr;
    rocksdb::DBImpl* impl_ = nullptr;
    // db's mutex
    rocksdb::InstrumentedMutex* mu_;
    // db status after processing an operation
    rocksdb::Status s_;
    rocksdb::IOStatus ios_;

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
  
    // right now, just put all sst files under one path
    rocksdb::DbPath db_path_;

    // maintain a mapping between sst_number and slot_number
    // sst_bit_map_[i] = j means sst_file with number j occupies the i-th slot
    // secondary node will update it when received a Sync rpc call from the upstream node
    std::unordered_map<int, uint64_t> sst_bit_map_;

    int big_sst_num_;
    
    rocksdb::FileSystem* fs_;

    std::atomic<uint64_t> log_apply_counter_;

    // client for making Sync rpc call to downstream node
    std::shared_ptr<SyncClient> sync_client_;

    // is rubble mode? If set to false, server runs a vanilla rocksdb
    bool is_rubble_ = false;
    bool is_head_ = false;
    bool is_tail_ = false;

    /* variables below are used for Sync method */
    // if true, means version edit received indicates a flush job
    bool is_flush_ = false; 
    // indicates if version edit corresponds to a trivial move compaction
    bool is_trivial_move_compaction_ = false;
    // number of added sst files
    int num_of_added_files_ = 0;
    // number of memtables get flushed in a flush job
    int batch_count_ = 0;
    // get the next file num of secondary, which is the maximum file number of the AddedFiles in the shipped vesion edit plus 1
    int next_file_num_ = 0;
};
