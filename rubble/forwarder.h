#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReaderWriter;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;

// client class used by the non-tail node in the chain to forward the op to the downstream node.
class Forwarder{
  public:
    Forwarder(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)), stream_(stub_->DoOp(&context_)) {
    };

    ~Forwarder(){
      std::cout << "forwarder destroyed\n";
    }

    // forward the op to the next node
    void Forward(const Op& op){
      if (!stream_->Write(op)) {
        stream_->WritesDone();
        Status s = stream_->Finish();
        std::cout << "Forward fail!"
                << " msg: " << s.error_message() 
                << " detail: " << s.error_details() 
                << " debug: " << context_.debug_error_string()
                << " shard: " << shard_idx << " client: " << client_idx << std::endl;
        assert(false);
      }
    }

    void ReadReply(OpReply *reply) {
      if (!stream_->Read(reply)) {
        stream_->Finish();
        assert(false);
      }
    }

    // forward the op to the next node
    void WritesDone() {
        stream_->WritesDone();
        stream_->Finish();
    }

    void set_idx(int s, int c) {
      shard_idx = s;
      client_idx = c;
    }
  
  private:
    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    int shard_idx = -1;
    int client_idx = -1;
    ClientContext context_;
    std::unique_ptr<RubbleKvStoreService::Stub> stub_;
    std::shared_ptr<ClientReaderWriter<Op, OpReply> > stream_;
};
