
#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <chrono>
#include <limits>
#include <thread>

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
using rubble::SingleOp;
using rubble::SingleOpReply;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;

class SyncKvStoreClient{
  public:
    SyncKvStoreClient(std::shared_ptr<Channel> channel, int batch_size)
        : stub_(RubbleKvStoreService::NewStub(channel)), batch_size_(batch_size){    
        stream_ = stub_->DoOp(&context_);
    //     thread_num_ = std::thread::hardware_concurrency();
    //     std::cout << thread_num_ << " Threads are supported\n";
    //     for(int i = 0 ; i < thread_num_; ++i){
    //       ClientContext ctx;
    //       ctxs_.push_back(std::move(&ctx));
    //       auto stub = RubbleKvStoreService::NewStub(channel);
    //       streams_.push_back(std::move(stub->DoOp(ctxs_[i])));
    //       stubs_.push_back(std::move(stub));
    //     }
    };

    ~SyncKvStoreClient(){
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
  void Get(const std::vector<std::string>& keys) {
    auto start_time = high_resolution_clock::now();

    for(const auto& key : keys){
      op_counter_.fetch_add(1);
      SingleOp* op = request_.add_ops();
      op->set_key(key);
      op->set_type(SingleOp::GET);
      op->set_id(op_counter_.load());

      if(request_.ops_size() == batch_size_){
        auto batch_start_time = high_resolution_clock::now();
        stream_->Write(request_);

        // Get the value for the sent key
        stream_->Read(&reply_);

        auto batch_end_time = high_resolution_clock::now();
        auto batch_process_time  = duration_cast<std::chrono::milliseconds>(batch_end_time - batch_start_time).count();
        std::cout << "Process bacth " << batch_size_ << " in " << std::to_string(batch_process_time) << " millisecs \n";

        assert(reply_.replies_size() == batch_size_);
        for(int i = 0; i < reply_.replies_size(); ++i){
          SingleOpReply reply = reply_.replies(i);
          if(!reply.ok()){
            std::cout << "Get -> " << request_.ops(i).key() << " Failed: " << reply.status() << "\n";
          }else{
            // std::cout << "Get -> " << request_.ops(i).key() << " , returned val : " << response.value() << std::endl;
          }
        }
        request_.clear_ops();
        reply_.clear_replies();
      }
    }
    auto end_time = high_resolution_clock::now();
    auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "send " << keys.size()<< " get ops in " << millisecs.count() << " millisecs \n";
  }

  void Put(const std::vector<std::pair<std::string, std::string>>& kvs){   
    auto start_time = high_resolution_clock::now();

    for(const auto& kv: kvs){
      op_counter_.fetch_add(1);

      SingleOp* op = request_.add_ops();
      op->set_key(kv.first);
      op->set_value(kv.second);
      op->set_type(SingleOp::PUT);
      op->set_id(op_counter_.load());

      if(request_.ops_size() == batch_size_){
        batch_counter_++;
        auto batch_start_time = high_resolution_clock::now();
        stream_->Write(request_);

        // Get the value for the sent key
        // stream_->Read(&reply_);
        auto batch_end_time = high_resolution_clock::now();
        auto batch_process_time  = duration_cast<std::chrono::milliseconds>(batch_end_time - batch_start_time).count();
        std::cout << "process bacth " << batch_counter_.load() << " in " << std::to_string(batch_process_time) << " millisecs \n";

        // assert(reply_.replies_size() == batch_size_);
        // for(int i = 0; i < reply_.replies_size(); ++i){
        //   SingleOpReply reply = reply_.replies(i);
        //   if(!reply.ok()){
        //     std::cout << "Put -> " << reply.key() << " Failed: " << reply.status() << "\n";
        //   }else{
        //     // std::cout << "Put -> " reply.key() << " succeeds " << std::endl;
        //   }
        // }
      
        // reply_.clear_replies();
        request_.clear_ops();
      }
    }
    auto end_time = high_resolution_clock::now();
    auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "send " << kvs.size()<< " put ops in " << millisecs.count() << " millisecs \n";
  }

  /**
   * @param index the start index of the requests vector to process
   * @param target the target count of requests to send
   */
  void DoOp(int index, int target){
    auto start_time = std::chrono::high_resolution_clock::now();
    int stream_idx = index/target;
    for(int i = 0; i < target; ++i){
      auto op = requests_[index + i];
      std::cout << " Get an op " << index + i << "\n";
      streams_[stream_idx]->Write(op);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::cout << "Thread " << std::this_thread::get_id() << " process time : " 
              << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() << std::endl;
  }

  void StartDoOp(){
    assert(requests_.size() > thread_num_);
    int target = requests_.size()/thread_num_;
    std::cout << "requests size : " << requests_.size() << " target : " << target << std::endl;

    for(unsigned int i = 0; i < thread_num_; ++i){
      auto new_thread = new std::thread(&SyncKvStoreClient::DoOp, this, i*target, target);
      threads_.emplace_back(new_thread);
    }

    for(const auto& t: threads_){
      t->join();
    }
  }

  void AddRequest(Op request){
    requests_.push_back(request);
  }

  int GetBatchSize(){
    return batch_size_;
  }

  private:
    Op request_;
    std::vector<Op> requests_;
    OpReply reply_;

    unsigned int thread_num_;
    std::vector<std::thread*> threads_;
    int batch_size_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;
    std::vector<ClientContext*> ctxs_;
    // Storage for the status of the RPC upon completion.
    Status status_;

    // The bidirectional,synchronous stream for sending/receiving messages.
    std::unique_ptr<ClientReaderWriter<Op, OpReply>> stream_;
    std::vector<std::unique_ptr<ClientReaderWriter<Op, OpReply>>> streams_; 

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;
    std::vector<std::unique_ptr<RubbleKvStoreService::Stub>> stubs_;

    std::atomic<uint64_t> op_counter_{0};

    std::atomic<uint64_t> batch_counter_{0};
};