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
        : stub_(RubbleKvStoreService::NewStub(channel)), stream_(stub_->DoOp(&context)) {
    };

    ~Forwarder(){
      std::cout << "forwarder destroyed\n";
    }

    // forward the op to the next node
    void Forward(const Op& op){
        if (!stream_->Write(op)) {
          std::cout << "forward fail!\n";
        }
    }

    // forward the op to the next node
    void WritesDone() {
        stream_->WritesDone();
        stream_->Finish();
    }
  
  private:
    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    ClientContext context;
    std::unique_ptr<RubbleKvStoreService::Stub> stub_;
    std::shared_ptr<ClientReaderWriter<Op, OpReply> > stream_;
};
