#pragma once

#include <vector>
#include <iostream>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "version_edit_sync.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
// using grpc::Status;
using version_edit_sync::VersionEditSyncService;
using version_edit_sync::VersionEditSyncRequest;
using version_edit_sync::VersionEditSyncReply;

using version_edit_sync::GetReply;
using version_edit_sync::GetRequest;
using version_edit_sync::PutReply;
using version_edit_sync::PutRequest;

using version_edit_sync::DeleteFile;
using version_edit_sync::NewFile;
using version_edit_sync::NewFile_FileMetaData;
using version_edit_sync::NewFile_FileMetaData_FileDescriptor;
using version_edit_sync::VersionEditToSync;

class VersionEditSyncClient {
  public:
    VersionEditSyncClient(std::shared_ptr<Channel> channel)
        : stub_(VersionEditSyncService::NewStub(channel)),
        to_primary_(false){};

    VersionEditSyncClient(std::shared_ptr<Channel> channel, bool to_primary)
        : stub_(VersionEditSyncService::NewStub(channel)),
        to_primary_(to_primary){};
    

    void DebugString(const VersionEditSyncRequest& request){
      VersionEditToSync edit =  request.edit();
    
      std::cout << "{LogNumber : " << edit.log_number() << " , PrevLogNumber : " << edit.prev_log_number();
      std::cout << " ColumnFamily : " << edit.column_family();
      std::cout << " , AddedFiles : [ ";
      for(int i = 0; i < edit.new__size(); i++){

        NewFile add = edit.new_(i);
        NewFile_FileMetaData meta = add.meta();
        NewFile_FileMetaData_FileDescriptor fd = meta.fd();
        std::cout << "{ Level :  " << add.level() << " , FileNumber : " << fd.file_number();
        std::cout << " , FileSize :  " << fd.file_size() << " , SmallestKey : " << meta.smallest_key() << " seqno: " << fd.smallest_seqno();
        std::cout << " , LargestKey : " << meta.largest_key() << " seqno: " << fd.largest_seqno();
       
        std::cout << " , oldest ancester time : "  << meta.oldest_ancestor_time();
        std::cout << " , file creation time : " << meta.file_creation_time();
        std::cout << " , file checksum : " << meta.file_checksum();
        std::cout << " , file checksum func name : " << meta.file_checksum_func_name() << " }, ";
      }
      std::cout << " ], DeletedFile : [ ";
      for(int i = 0; i < edit.del_size(); i++){
        DeleteFile del = edit.del(i);
        std::cout << " { FileLevel : " << del.level() << " , FileNumber : " <<  del.file_number() << " }, ";
      }
      std::cout << " ] }";
    }
   
    std::string VersionEditSync(const VersionEditSyncRequest& request) {

      VersionEditSyncReply reply;
      // DebugString(request);
      ClientContext context;
      grpc::Status status = stub_->VersionEditSync(&context, request, &reply);

      if (status.ok()) {
        return reply.message();
      } else {
        std::cout << status.error_code() << ": " << status.error_message()
                    << std::endl;
          return "RPC failed";
      }
    }

  // Requests each key in the vector and displays the key and its corresponding
  // value as a pair
  grpc::Status Get(const std::vector<std::string>& keys, std::vector<std::string>& vals);
  grpc::Status Put(const std::pair<std::string, std::string>& kv);

  private:
    std::unique_ptr<VersionEditSyncService::Stub> stub_ = nullptr;
    // Is this client sending kv to the primary? If not, it's sending kv to the secondary
    bool to_primary_ = false;
    std::atomic<uint64_t> put_count_{0};
};