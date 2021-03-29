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
using rubble::Reply;
using rubble::OpReply;
using rubble::Op_OpType_Name;


// client class used by the tail node in the chain to send the true reply back to the replicator
class ReplyClient{
  public:
    ReplyClient(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)) {    
        stream_ = stub_->SendReply(&context_);
    };

    ~ReplyClient(){
    }

    // send the reply to the replicator
    void SendReply(const OpReply& reply){
       stream_->Write(reply);
      //  std::cout << "sendReply client on reply: " << reply.ok() << "\n";
    }
  
  private:
    OpReply reply_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;

    // Storage for the status of the RPC upon completion.
    Status status_;

    // The bidirectional,synchronous stream for sending/receiving messages.
    std::unique_ptr<ClientReaderWriter<OpReply, Reply>> stream_;
    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;

    std::atomic<uint64_t> op_counter_{0};
};
