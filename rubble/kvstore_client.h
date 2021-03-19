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
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientReaderWriter;
using grpc::CompletionQueue;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;
using rubble::Op_OpType_Name;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;

// client class used to send db operations to the server
class KvStoreClient{
  enum class Type {
        READ = 1,
        WRITE = 2,
        CONNECT = 3,
        WRITES_DONE = 4,
        FINISH = 5
    };

  public:
    KvStoreClient(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)) {
      grpc_thread_.reset(new std::thread(std::bind(&KvStoreClient::AsyncCompleteRpc, this)));
      // aysnc_do_op_thread_.reset(new std::thread(&KvStoreClient::AsyncDoOp, this));
      stream_ = stub_->AsyncDoOp(&context_, &cq_,
                                   reinterpret_cast<void*>(Type::CONNECT));
      
      // sync_stream_ = stub_->DoOp(&sync_context_);
    };

    ~KvStoreClient(){
        std::cout << "Shutting down client...." << std::endl;
        cq_.Shutdown();
        grpc_thread_->join();
    }

  Status SyncDoOpDone(){
    sync_stream_->WritesDone();
    Status status = sync_stream_->Finish();
    if (!status.ok()) {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      std::cout << "RPC failed";
    }
    return status;
  }

  // Requests each key in the vector and displays the key and its corresponding
  // value as a pair
  void SyncDoGet(const std::string& key, std::string& val) {
 
    // Key we are sending to the server.
    Op request;
    request.set_key(key);
    request.set_type(Op::GET);
    sync_stream_->Write(request);

    // Get the value for the sent key
    OpReply response;
    sync_stream_->Read(&response);

    if(!response.ok()){
      std::cout << "Get key : " << key << " Failed: " << response.status() << "\n";
    }else{
        val = response.value();
        std::cout << "Get Key : " << key << " returned val : " << val << std::endl;
    }
    return ;
  }

  void SyncDoPut(const std::pair<std::string, std::string>& kv){   
      Op request;
      request.set_key(kv.first);
      request.set_value(kv.second);
      request.set_type(Op::PUT);
      
      sync_stream_->Write(request);
  }

  void SetKvClient(bool is_kvclient){
    is_forwarder_ = !is_kvclient;
  }
  // Put is client-to-server streaming async rpc
  // since it's a chain replication, we don't need a reply when we send a put operation to a server
  // instead, the true reply is returned by the tail node in the chain to the client
  void AsyncDoPuts(const std::vector<std::pair<std::string, std::string>>& kvs){
    assert(!op_counter_);
    std::thread worker(std::bind(&KvStoreClient::AsyncDoOps, this));
    {
      std::unique_lock<std::mutex> lk{mu_};
      if(!requests_.empty())
        cv_empty_.wait(lk, [&](){return empty_.load();});
      op_counter_ = 0;
      Op request;
      for(const auto& kv : kvs){
        request.set_type(Op::PUT);
        request.set_key(kv.first);
        request.set_value(kv.second);
        // request.set_id(distr_(eng_));
        requests_.push_back(std::move(request));
      }
      empty_.store(false);
    }
    // std::cout << "data stored\n";
    start_time_ = high_resolution_clock::now();
    // std::cout << "signals data ready for processing \n";
    cv_empty_.notify_one();
    worker.join();
  }

  // Get should return back a reply, this is sending a Get request, the actual returned reply is in ReadReplyForGet
  void AsyncDoGets(const std::vector<std::string>& keys){
    std::thread worker(std::bind(&KvStoreClient::AsyncDoOps, this));
    {
      std::unique_lock<std::mutex> lk(mu_);
      if(!requests_.empty())
        cv_empty_.wait(lk, [&](){return empty_.load();});
      op_counter_ = 0;
      Op request;
      for(const auto& key : keys){
        request.set_type(Op::GET);
        request.set_key(key);
        // request.set_id(distr_(eng_));
        requests_.push_back(std::move(request));
      }
      empty_.store(false);
    }
    start_time_ = high_resolution_clock::now();
    cv_empty_.notify_one();
    worker.join();
  }

  //tell the server we're done sending the ops
  void WritesDone(){
      stream_->WritesDone(reinterpret_cast<void*>(Type::WRITES_DONE));
      return;
  }

  // used by server node to forward op 
  void ForwardOp(const Op& op){
    request_ = op;
    // check if it's ready to call Write
    if(!ready_.load()){ // If not ready, gonna wait for cv_.notify
        // std::cout << "[m] wait : " <<  op_counter_ <<"\n"; 
        // std::cout <<  Op_OpType_Name(request_.type()) << " -> " << request_.key() << std::endl;
        std::unique_lock<std::mutex> lk(mu_);
        // std::cout << "[m] lock\n";
        // wait until got notified when cq received a tag and ready_ set to true
        cv_ready_.wait(lk,  [&](){return ready_.load();});
        // std::cout << "[m] Notified\n";
    }
    
    ready_.store(false);
    // This is important: You can have at most one write or at most one read
    // at any given time. The throttling is performed by gRPC completion
    // queue.
    // Only one write may be outstanding at any given time. This means that
    // after calling Write, one must wait to receive \a tag from the completion
    // queue BEFORE calling Write again.
    // Because this stream is bidirectional, you *can* have a single read
    // and a single write request queued for the same stream. Writes and reads
    // are independent of each other in terms of ordering/delivery.
    //std::cout << " ** Sending request: " << user << std::endl;
    stream_->Write(request_, reinterpret_cast<void*>(Type::WRITE));  
    return;
  }


  // could be run in multiple threads, used by the kvstore client to send requests
  void AsyncDoOps(){
    if(requests_.empty()){
      // std::cout << "[w] empty queue\n";
      // op_counter != 0 calculate the throughput
      if(op_counter_){
        end_time_ = high_resolution_clock::now();
        auto milisecs = duration_cast<std::chrono::milliseconds>(end_time_ - start_time_);
        std::cout << "Druration(millisecs): " << (int)(milisecs.count()) << std::endl;
        // if(!milisecs.count()){
        //   std::cout << "Throughput : " << (op_counter_)/(milisecs.count()/1000) << " op/s" << std::endl;
        // }
        // reset op counter
        op_counter_.store(0);
        return;
      }else{ // start of a new round
        std::unique_lock<std::mutex> lk(mu_);
        // std::cout << "worker wait for data\n";
        cv_empty_.wait(lk, [&](){return !empty_.load();});
        // std::cout << "[w] Notified \n";
      }
    }
    // std::cout <<  Op_OpType_Name(request_.type()) << " -> " << request_.key() << std::endl;
    request_ = requests_.front();
    // requests_.pop_front();
    
    op_counter_++;
    stream_->Write(request_, reinterpret_cast<void*>(Type::WRITE));
  }

  private:
    // read a reply back for a Get request
     void ReadReplyForGet() {
        // The tag is the link between our thread (main thread) and the completion
        // queue thread. The tag allows the completion queue to fan off
        // notification handlers for the specified read/write requests as they
        // are being processed by gRPC.
        stream_->Read(&reply_, reinterpret_cast<void*>(Type::READ));
        if(reply_.ok()){
          std::cout << "returned val for key " << reply_.key() << " : " << reply_.value() << std::endl;
        }else{
          std::cout << reply_.status() << std::endl;
        }
    }

    // Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
    // and processes them until there are no more (or when the completion queue
    // is shutdown).
    void AsyncCompleteRpc(){
      void* got_tag;
      bool ok = false;

      // Block until the next result is available in the completion queue "cq".
      while (cq_.Next(&got_tag, &ok)) {
        // Verify that the request was completed successfully. Note that "ok"
        // corresponds solely to the request for updates introduced by Finish().
        GPR_ASSERT(ok);

        switch (static_cast<Type>(reinterpret_cast<long>(got_tag))) {
          case Type::READ:
              // in this case, means we get a reply from the server, which is for a Get operation
              if(reply_.ok()){
                std::cout << "Get for key : " << reply_.key() << " , returned value : " << reply_.value() << std::endl;
              }else{
                std::cout << "Get for key : " << reply_.key() << " , returned status : " << reply_.status() << std::endl;
              }
              // notify that we're ready to process next request
              if(is_forwarder_){
                ready_.store(true);
                cv_ready_.notify_one();
              }else{
                AsyncDoOps();
              }
              break;
          case Type::WRITE:
              switch (request_.type())
              {
              case Op::GET:
                std::cout << "Get -> " << request_.key() << std::endl;
                ReadReplyForGet();
                // AysncDoOp() /* call Write here ? */
                break;
              case Op::PUT:
                // std::cout << "Put -> (" << request_.key() << ", " << request_.value() << ")\n";
                // received a tag on the cq, we can start a new Write
                if(is_forwarder_){
                  ready_.store(true);
                  // std::cout << "[g] Notify\n";
                  cv_ready_.notify_one();
                  // ForwardOp();
                }else{ //it's kvstore client 
                //  std::cout << "[g] polled :" << request_.key()  << std::endl;
                  requests_.pop_front();
                  AsyncDoOps();
                }
                break;
              
              case Op::DELETE:
                std::cout << "Delete : " << request_.key() << std::endl;
                ready_.store(true);
                cv_ready_.notify_one();
                break;  
              
              case Op::UPDATE:
                std::cout << "Update : (" << request_.key() << ", " << request_.value() << ")\n";
                ready_.store(true);
                cv_ready_.notify_one();
                break;

              default:
                break;
              }
              break;
          case Type::CONNECT:
              // std::cout << "Server connected." << std::endl;
              break;
          case Type::WRITES_DONE:
              std::cout << "writesdone sent,sleeping 5s" << std::endl;
              stream_->Finish(&finish_status_, reinterpret_cast<void*>(Type::FINISH));
              break;
          case Type::FINISH:
              std::cout << "Client finish status:" << finish_status_.error_code() << ", msg:" << finish_status_.error_message() << std::endl;
              //context_.TryCancel();
              cq_.Shutdown();
              break;
          default:
              std::cerr << "Unexpected tag " << got_tag << std::endl;
              assert(false);
        }
      }
    }

    Op request_;
    OpReply reply_;
    
    //whether this is a fowarder or a kvstore client
    bool is_forwarder_ = true;
    // hold the requests client needs to process
    std::deque<Op> requests_;
    std::mutex mu_;

    //used by forwarder to wait for cq received a new tag, then it can start a new Write(forward a op)
    std::condition_variable cv_ready_;
    std::atomic<bool> ready_ {false};
    // used by the kvstore client to wait for the requests queue to become empty(client is done processing all the previous requests)
    std::condition_variable cv_empty_;
    std::atomic<bool> empty_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;
    // Context used for doing sync ops
    ClientContext sync_context_;

    // Storage for the status of the RPC upon completion.
    Status status_;

    // The bidirectional,synchronous stream for sending/receiving messages.
    std::unique_ptr<ClientReaderWriter<Op, OpReply>> sync_stream_;
    // The bidirectional, asynchronous stream
    std::unique_ptr<ClientAsyncReaderWriter<Op, OpReply>> stream_;

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;

    // Thread that notifies the gRPC completion queue tags.
    std::unique_ptr<std::thread> grpc_thread_;

    // std::unique_ptr<std::thread> async_dp_op_thread_;
    // Finish status when the client is done with the stream.
    grpc::Status finish_status_ = grpc::Status::OK;

    std::atomic<uint64_t> op_counter_{0};

    // std::random_device rd;     //Get a random seed from the OS entropy device, or whatever
    // std::mt19937_64 eng_(rd());
    // std::uniform_int_distribution<unsigned long> distr_;

    std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
    std::chrono::time_point<std::chrono::high_resolution_clock> end_time_;
};
