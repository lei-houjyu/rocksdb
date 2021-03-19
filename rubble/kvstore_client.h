#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <deque>
#include <thread>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include <random>
#include <limits>

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
using rubble::Op_OpType_Name;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;

// client class used to either to send db operations to the server by the kvstore client(is_forwarder_ set to false)
// or used by the nodes in the chain to forward op to downstream node(is_forwarder_ set to true)
class KvStoreClient{
  public:
    KvStoreClient(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)) {    
        stream_ = stub_->DoOp(&context_);
    };

    ~KvStoreClient(){
    }

  // tell the server we're done
  Status Done(){
    stream_->WritesDone();
    Status status = stream_->Finish();
    if (!status.ok()) {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      std::cout << "RPC failed";
    }
    return status;
  }

  // Requests each key in the vector and displays the key and its corresponding
  // value as a pair
  void SyncDoGets(const std::vector<std::string>& keys) {
    auto start_time = high_resolution_clock::now();
    for(const auto& key:keys){
      // Keys we are sending to the server.
      Op request;
      request.set_key(key);
      request.set_type(Op::GET);
      stream_->Write(request);
      // Get the value for the sent key
      OpReply response;
      stream_->Read(&response);
      if(!response.ok()){
        std::cout << "Get -> " << key << " ,Failed: " << response.status() << "\n";
      }else{
        std::cout << "Get -> " << key << " ,returned val : " << response.value() << std::endl;
      }
    }
    auto end_time = high_resolution_clock::now();
    auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "send " << keys.size()<< " get ops in " << millisecs.count() << " millisecs \n";
  }

  void SyncDoPuts(const std::vector<std::pair<std::string, std::string>>& kvs){   
    auto start_time = high_resolution_clock::now();
    for(const auto& kv: kvs){
      Op request;
      request.set_key(kv.first);
      request.set_value(kv.second);
      request.set_type(Op::PUT);
      stream_->Write(request);
    }
    auto end_time = high_resolution_clock::now();
    auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "send " << kvs.size()<< " put ops in " << millisecs.count() << " millisecs \n";
  }

  // used by server node to forward op 
  void ForwardOp(const Op& op){
    request_ = op;
    stream_->Write(request_);  
    return;
  }

  private:
    Op request_;
    OpReply reply_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;

    // Storage for the status of the RPC upon completion.
    Status status_;

    // The bidirectional,synchronous stream for sending/receiving messages.
    std::unique_ptr<ClientReaderWriter<Op, OpReply>> stream_;
    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;

    std::atomic<uint64_t> op_counter_{0};
};
