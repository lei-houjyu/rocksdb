#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>

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
        : stub_(RubbleKvStoreService::NewStub(channel)){
      grpc_thread_.reset(
          new std::thread(std::bind(&KvStoreClient::AsyncCompleteRpc, this)));
      stream_ = stub_->AsyncDoOp(&context_, &cq_,
                                   reinterpret_cast<void*>(Type::CONNECT));
      
      // sync_stream_ = stub_->DoOp(&sync_context_);
    };

    ~KvStoreClient(){
        std::cout << "Shutting down client...." << std::endl;
        cq_.Shutdown();
        grpc_thread_->join();
    }

  void DoOp(const Op& op){
    request_ = op;
    AsyncDoOp();
  }


  void Get(const std::vector<std::string>& keys){
    for(const auto& key : keys){
        AsyncDoGet(key);
    }
  }

  void Put(const std::vector<std::pair<std::string, std::string>>& kvs){
    for(const auto& kv: kvs){
      AsyncDoPut(kv.first, kv.second);
    }
  }

  Status SyncOpDone(){
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

  // Put is client-to-server streaming async rpc
  // since it's a chain replication, we don't need a reply when we send a put operation to a server
  // instead, the true reply is returned by the tail node in the chain to the client
  void AsyncDoPut(const std::string& key, const std::string& val){
    request_.set_key(key);
    request_.set_value(val);
    request_.set_type(Op::PUT);
    AsyncDoOp();
  }

  // Get should return back a reply, this is sending a Get request, the actual returned reply is in ReadReplyForGet
  void AsyncDoGet(const std::string& key) {
      request_.set_key(key);
      request_.set_type(Op::GET);
      AsyncDoOp();
  }

  //tell the server we're done sending the ops
  void WritesDone(){
      stream_->WritesDone(reinterpret_cast<void*>(Type::WRITES_DONE));
      return;
  }

  // could be run in multiple threads
  void AsyncDoOp(){
    op_counter_++;
    {
    // check if it's ready to call Write
      if(!ready_.load()){ // If not ready, gonna wait for cv_.notify
          std::cout << "[m] wait : " <<  op_counter_ <<"\n"; 
          // std::cout <<  Op_OpType_Name(request_.type()) << " -> " << request_.key() << std::endl;
          std::unique_lock<std::mutex> lk(mu_);
          // std::cout << "[m] lock\n";
          // wait until got notified when cq received a tag and ready_ set to true
          cv_.wait_for(lk, std::chrono::milliseconds(100), [&](){return ready_.load();});
          // std::cout << "[m] Notified\n";
          // ready_.store(false);
      }
    }
      
    ready_.store(false);
    // assert(request_.type() == Op::PUT);
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
    
    // std::cout << "[Write " << Op_OpType_Name(request_.type()) << " op with key : " << request_.key() << "]\n";
    // if(ready_.load()){
    // }
    return;
  }

  void SetRequest(const Op& op){
    request_ = op;
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
              ready_.store(true);
              cv_.notify_one();
              break;
          case Type::WRITE:
              switch (request_.type())
              {
              case Op::GET:
                std::cout << "Get -> " << request_.key() << std::endl;
                ReadReplyForGet();
                break;
              case Op::PUT:
                // std::cout << "[g] polled :" << request_.name() << std::endl;
                // std::cout << "Put -> (" << request_.key() << ", " << request_.value() << ")\n";
                // received a tag on the cq, notify the main thread that we can start a new Write
                ready_.store(true);
                // std::cout << "[g] Notify\n";
                cv_.notify_one();

                // stream_->Write(request_, reinterpret_cast<void*>(Type::WRITE));  
                break;
              
              case Op::DELETE:
                std::cout << "Delete : " << request_.key() << std::endl;
                ready_.store(true);
                cv_.notify_one();
                break;  
              
              case Op::UPDATE:
                std::cout << "Update : (" << request_.key() << ", " << request_.value() << ")\n";
                ready_.store(true);
                cv_.notify_one();
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
              // assert(false);
              break;
        }
      }
    }

    // Container for the data we expect from the server.
    Op request_;
    // reply returned from the server for the request sent
    OpReply reply_;

    // indicator of whether the client is ready to call Write
    // at the initial state, we can call Write, so ready_ defaults to true
    std::atomic<bool> ready_ {true};

    std::mutex mu_;

    std::condition_variable_any cv_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;

    // Context used for doing sync ops
    ClientContext sync_context_;

    // Storage for the status of the RPC upon completion.
    Status status_;

    // The bidirectional,synchronous stream for sending/receiving messages.
    std::unique_ptr<ClientReaderWriter<Op, OpReply>> sync_stream_;

    // The bidirectional, asynchronous stream for sending/receiving messages.
    std::unique_ptr<ClientAsyncReaderWriter<Op, OpReply>> stream_;

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<RubbleKvStoreService::Stub> stub_ = nullptr;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;

    // Thread that notifies the gRPC completion queue tags.
    std::unique_ptr<std::thread> grpc_thread_;

    // Finish status when the client is done with the stream.
    grpc::Status finish_status_ = grpc::Status::OK;

    std::atomic<uint64_t> op_counter_{0};
};
