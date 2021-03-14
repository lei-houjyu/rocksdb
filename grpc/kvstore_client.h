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
using grpc::Status;
using grpc::ClientAsyncResponseReader;
using grpc::ClientAsyncReaderWriter;
using grpc::CompletionQueue;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;

// Client used by user to make Put/Get requests to the db server
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
          new std::thread([=]() { &KvStoreClient::AsyncCompleteRpc }));
      stream_ = stub_->AsyncDoOp(&context_, &cq_,
                                   reinterpret_cast<void*>(Type::CONNECT));
      sync_stream_ = stub_->DoOp(&context_);
    };

    ~KvStoreClient(){
        std::cout << "Shutting down client...." << std::endl;
        cq_.Shutdown();
        grpc_thread_->join();
    }

  
  Status Get(const std::vector<std::string>& keys, std::vector<std::string>& vals){
    for(const auto& key : keys){
        AsyncDoGet(key);
    }
  }

  Status Put(const std::vector<std::pair<std::string, std::string>> kvs){
    for(const auto& kv: kvs){
      AsyncDoPut(kv.first, kv.second);
    }
  }

  // shouldb be called when we're done with all operations
  void DoneWithOps(){
    //Notify the server that we're done with writes
    stream_->WritesDone(reinterpret_cast<void*>(Type::WRITES_DONE));
  }

  // Requests each key in the vector and displays the key and its corresponding
  // value as a pair
  Status SyncDoGet(const std::vector<std::string>& keys, std::vector<std::string>& vals) {
 
    for (const auto& key : keys) {
      // Key we are sending to the server.
      Op request;
      request.set_key(key);
      request.set_type(Op::GET);
      sync_stream_->Write(request);

      // Get the value for the sent key
      OpReply response;
      sync_stream_->Read(&response);

      vals.emplace_back(response.value());
      std::cout << key << " : " << response.value() << "\n";
    }

    sync_stream_->WritesDone();
    Status status = sync_stream_->Finish();
    if (!status.ok()) {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      std::cout << "RPC failed";
    }
    return status;
  }

  Status SyncDoPut(const std::vector<std::pair<std::string, std::string>>& kvs){
  
    for(const auto& kv: kvs){
        
      Op request;
      request.set_key(kv.first);
      request.set_value(kv.second);
      request.set_type(Op::PUT);
      
      sync_stream_->Write(request);

      OpReply reply;
      sync_stream_->Read(&reply);

      // if(put_count_ % 1000 == 0 ){
        // std::cout << " Put ----> " ;
        // if(to_primary_){
        //   std::cout << "Primary   ";
        // }else{
        //   std::cout << "Secondary ";
        // }
      std::cout <<" ( " << kv.first << " ," << kv.second << " ) , status : " ;
      if(reply.ok()){
        std::cout << "Succeeds";
      } else{
        
        std::cout << "Failed ( " << reply.status() << " )";
      }
      std::cout << "\n";  

        // }
    }

    // Signal the client is done with the writes (half-close the client stream).
    sync_stream_->WritesDone();
    Status status = sync_stream_->Finish();
    if(!status.ok()){
        std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
        std::cout << "RPC failed";
    }
    return s;
  }

  void AsyncDoPut(const std:string& key, const string& val){

    request_.set_key(key);
    request_.set_value(val);
    request_.set_type(Op::PUT);
    AsyncDoOp();
  }

  void AsyncDoGet(const std::string& key) {

      // Data we are sending to the server.
      request_.set_key(key);
      request_.set_value(Op::GET);
      AsyncDoOp();

  }

  bool AsyncDoOp(){

        // This is important: You can have at most one write or at most one read
        // at any given time. The throttling is performed by gRPC completion
        // queue. If you queue more than one write/read, the stream will crash.
        // Because this stream is bidirectional, you *can* have a single read
        // and a single write request queued for the same stream. Writes and reads
        // are independent of each other in terms of ordering/delivery.
        //std::cout << " ** Sending request: " << user << std::endl;
        stream_->Write(request_, reinterpret_cast<void*>(Type::WRITE));
        return true;
    
  }

  private:
     void AsyncDoOpRequestNextMessage() {

        // The tag is the link between our thread (main thread) and the completion
        // queue thread. The tag allows the completion queue to fan off
        // notification handlers for the specified read/write requests as they
        // are being processed by gRPC.
        stream_->Read(&reply_, reinterpret_cast<void*>(Type::READ));
    }

    // a thread Looping to pull responses from the cq while listening for completed responses.
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
              switch (reply_.type())
              {
              case Op::Get :
                /* code */
                break;
              case Op::PUT :
                std::cout << "Read a new message:" << reply_.value() << std::endl;
                break;
              case Op::DELETE :
                break;
              case Op::UPDATE :
                break;
              default:
                break;
              }
              break;
          case Type::WRITE:
              std::cout << "Sent message :" << request_.name() << std::endl;
              AsyncDoOpNextMessage();
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
    // Container for the data we expect from the server.
    OpReply reply_;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context_;

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
};
