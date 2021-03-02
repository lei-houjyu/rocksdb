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

class VersionEditSyncClient {
  public:
    VersionEditSyncClient(std::shared_ptr<Channel> channel)
        : stub_(VersionEditSyncService::NewStub(channel)),
        to_primary_(false){};

    VersionEditSyncClient(std::shared_ptr<Channel> channel, bool to_primary)
        : stub_(VersionEditSyncService::NewStub(channel)),
        to_primary_(to_primary){};
    
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