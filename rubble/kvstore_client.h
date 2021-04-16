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
using rubble::SingleOp;
using rubble::SingleOpReply;
using rubble::SingleOp_OpType_Name;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;

// client class used to either to send db operations to the server by the kvstore client(is_forwarder_ set to false)
// or used by the nodes in the chain to forward op to downstream node(is_forwarder_ set to true)
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
      stream_ = stub_->AsyncDoOp(&context_, &cq_,
                                   reinterpret_cast<void*>(Type::CONNECT));
    };

    ~KvStoreClient(){
        std::cout << "Shutting down client...." << std::endl;
        cq_.Shutdown();
        grpc_thread_->join();
    }

  //tell the server we're done sending the ops
  void WritesDone(){
      stream_->WritesDone(reinterpret_cast<void*>(Type::WRITES_DONE));
      return;
  }

  // used by server node to forward op asynchronously
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
      if(op_counter_){
        if(op_counter_ >= 100000){
          // do calcaulate when we send more than 100000 requests at once
          end_time_ = high_resolution_clock::now();
          auto secs = duration_cast<std::chrono::seconds>(end_time_ - start_time_);
          // std::cout << "Druration(millisecs): " << (int)(milisecs.count()) << std::endl;
          if(secs.count() >= 1){
           std::cout << "Throughput : " << op_counter_/secs.count() << " op/s" << std::endl;
          }
        }
        // reset op counter
        op_counter_.store(0);
        return;
      }else if(!op_counter_){ 
        std::cout << "Don't have any requests to process\n";
        return;
      }
    }
    // std::cout <<  Op_OpType_Name(request_.type()) << " -> " << request_.key() << std::endl;
    request_ = requests_.front();
    op_counter_++;
    stream_->Write(request_, reinterpret_cast<void*>(Type::WRITE));
  }

  void SetKvClient(bool is_kvclient){
    is_forwarder_ = !is_kvclient;
  }

  void SetStartTime(std::chrono::time_point<std::chrono::high_resolution_clock> start_time){
    start_time_ = start_time;
  }
  
  // before call AsyncDoOps, push the request into the queue.
  void AddRequests(const Op& request){
    requests_.push_back(std::move(request));
  }
  
  private:
    // read a reply back for a Get request
     void ReadReplyForGet() {
        // The tag is the link between our thread (main thread) and the completion
        // queue thread. The tag allows the completion queue to fan off
        // notification handlers for the specified read/write requests as they
        // are being processed by gRPC.
        stream_->Read(&reply_, reinterpret_cast<void*>(Type::READ));
        for(const auto& reply: reply_.replies()) {
          if(reply.ok()){
            std::cout << "returned val for key " << reply.key() << " : " << reply.value() << std::endl;
          }else{
            std::cout << reply.status() << std::endl;
          }
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
        // GPR_ASSERT(ok);

        switch (static_cast<Type>(reinterpret_cast<long>(got_tag))) {
          case Type::READ:
              // in this case, means we get a reply from the server, which is for a Get operation
              for(const auto& reply: reply_.replies()) {
                if(reply.ok()){
                  std::cout << "Get for key : " << reply.key() << " , returned value : " << reply.value() << std::endl;
                }else{
                  std::cout << "Get for key : " << reply.key() << " , returned status : " << reply.status() << std::endl;
                }
              }
              // notify that we're ready to process next request
              if(is_forwarder_){
                ready_.store(true);
                cv_ready_.notify_one();
              }else{
                requests_.pop_front();
                AsyncDoOps();
              }
              break;
          case Type::WRITE:
              // assume each batch contains same type of op
              if (request_.ops_size()) {
                switch (request_.ops(0).type())
                {
                case SingleOp::GET:
                  // std::cout << "Get -> " << request_.key() << std::endl;
                  ReadReplyForGet();
                  // AysncDoOp() /* call Write here ? */
                  break;
                case SingleOp::PUT:
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

                case SingleOp::DELETE:
                  //TODO
                  break;  

                case SingleOp::UPDATE:
                //TODO
                  break;

                default:
                  break;
                }
              }
              break;
          case Type::CONNECT:
              std::cout << "Server connected." << std::endl;
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
    // hold the requests client needs to process
    std::deque<Op> requests_;
    //whether this is a fowarder or a kvstore client, set to true by default
    bool is_forwarder_ = true;
    std::mutex mu_;

    //used by forwarder to wait for cq received a new tag, then it can start a new Write(forward a op)
    std::condition_variable cv_ready_;
    std::atomic<bool> ready_ {false};
    // used by the kvstore client to wait for the requests queue to become empty(client is done processing all the previous requests)
    // std::condition_variable cv_empty_;
    // std::atomic<bool> empty_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;
    // Context used for doing sync ops
    ClientContext sync_context_;

    // Storage for the status of the RPC upon completion.
    Status status_;

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

    //for counting op/s
    std::chrono::time_point<std::chrono::high_resolution_clock> start_time_;
    std::chrono::time_point<std::chrono::high_resolution_clock> end_time_;
};