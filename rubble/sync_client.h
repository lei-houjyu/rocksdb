#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <limits>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::ClientAsyncReaderWriter;
using grpc::CompletionQueue;

using rubble::RubbleKvStoreService;
using rubble::SyncRequest;
using rubble::SyncReply;

// client responsible for making Sync rpc call to the secondary, used by primary instance 
class SyncClient {
  enum class Type {
        READ = 1,
        WRITE = 2,
        CONNECT = 3,
        WRITES_DONE = 4,
        FINISH = 5
    };
  
  public:
    SyncClient(std::shared_ptr<Channel> channel);

    ~SyncClient();

  void Sync(const std::string& args);

  private:
    // read a reply back for a sync request
    void GetSyncReply();

    // check the SyncReply status
    bool CheckReply(const SyncReply& reply);

    // Runs a gRPC completion-queue processing thread. Checks for 'Next' tag
    // and processes them until there are no more (or when the completion queue
    // is shutdown).
    void AsyncCompleteRpc();

    SyncRequest request_;
    SyncReply reply_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::atomic<bool> ready_ {true};

    std::mutex read_lock_;
    std::condition_variable cv_read_;
    std::atomic<bool> read_ready_{true};

    ClientContext context_;

    // The bidirectional, asynchronous stream
    std::unique_ptr<ClientAsyncReaderWriter<SyncRequest, SyncReply>> stream_;

    std::unique_ptr<ClientReaderWriter<SyncRequest, SyncReply>> sync_stream_;

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