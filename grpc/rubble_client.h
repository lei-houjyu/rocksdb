#pragma once

#include <vector>
#include <iostream>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
// using grpc::Status;
using rubble::RubbleKvStoreService;
using rubble::SyncRequest;
using rubble::SyncReply;

using rubble::GetReply;
using rubble::GetRequest;
using rubble::PutReply;
using rubble::PutRequest;

// a rubble client could be used by a user to make Put/Get request to the server
// or it could be used by the primary to do Sync rpc call to the secondary
class RubbleClient {
  public:
    RubbleClient(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)){};
    
    RubbleClient(std::shared_ptr<Channel> channel, bool to_primary)
        : stub_(RubbleKvStoreService::NewStub(channel)),
        to_primary_(to_primary){};

    std::string Sync(const SyncRequest& request){

      SyncReply reply;
      // DebugString(request);
      ClientContext context;
      std::cout << "--------------- Calling sync with args : " << request.args() <<" ----------\n";
      grpc::Status status = stub_->Sync(&context, request, &reply);
      std::cout << "--------------- Return from sync --------------\n";
      if (status.ok()) {
        return reply.message();
      } else {
        std::cout << status.error_code() << ": " << status.error_message()
                    << std::endl;
          return "RPC failed";
      }
    }
  grpc::Status Put(const std::pair<std::string, std::string>& kv);
  grpc::Status Get(const std::vector<std::string>& keys, std::vector<std::string>& vals);

  private:
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;
     // Is this client sending kv to the primary? If not, it's sending kv to the secondary
    bool to_primary_ = false;
    std::atomic<uint64_t> put_count_{0};
};