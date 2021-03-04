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

// client responsible for making Sync rpc call to the secondary, used by primary instance 
class SyncClient {
  public:
    SyncClient(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)){};
    

    std::string Sync(const SyncRequest& request){

      SyncReply reply;
      // DebugString(request);
      ClientContext context;
      // std::cout << "--------------- Calling sync with args : " << request.args() <<" ----------\n";
      grpc::Status status = stub_->Sync(&context, request, &reply);
      // std::cout << "--------------- Return from sync --------------\n";
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
};