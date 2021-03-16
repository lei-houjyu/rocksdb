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
      ClientContext context;
      grpc::Status status = stub_->Sync(&context, request, &reply);
      if (status.ok()) {
        return reply.message();
      } else {
        std::cout << status.error_code() << ": " << status.error_message()
                    << std::endl;
          return "RPC failed";
      }
    }

  private:
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;
};