#pragma once

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <iomanip>
#include <string> 
#include <memory>
#include <unordered_map>
#include <atomic>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/alarm.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"
#include "reply_client.h"
#include "forwarder.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "sync_service_impl.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerReaderWriter;
using grpc::ServerAsyncReaderWriter;
using grpc::Status;
using grpc::StatusCode;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;
using rubble::SingleOp;
using rubble::SingleOpReply;
using rubble::SingleOp_OpType_Name;

using std::chrono::time_point;
using std::chrono::high_resolution_clock;

class CallDataBase {
public:
  CallDataBase(SyncServiceImpl* service, 
                ServerCompletionQueue* cq, 
                rocksdb::DB* db, 
                std::shared_ptr<Channel> channel)
   :service_(service), cq_(cq), db_(db), 
    channel_(channel){

   }

  virtual void Proceed(bool ok) = 0;

  virtual void HandleOp() = 0;

protected:

  // db instance
  rocksdb::DB* db_;

  // status of the db after performing an operation.
  rocksdb::Status s_;

  const rocksdb::ImmutableDBOptions* db_options_;

  std::shared_ptr<Channel> channel_ = nullptr;
  std::shared_ptr<Forwarder> forwarder_ = nullptr;
  // client for sending the reply back to the replicator
  std::shared_ptr<ReplyClient> reply_client_ = nullptr;
  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  SyncServiceImpl* service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue* cq_;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // What we get from the client.
  Op request_;
  // What we send back to the client.
  OpReply reply_;

};

// CallData for Bidirectional streaming rpc 
class CallDataBidi : CallDataBase {

 public:

  // Take in the "service" instance (in this case representing an asynchronous
  // server) and the completion queue "cq" used for asynchronous communication
  // with the gRPC runtime.
  CallDataBidi(SyncServiceImpl* service, 
                ServerCompletionQueue* cq, 
                rocksdb::DB* db,
                std::shared_ptr<Channel> channel);

  // async version of DoOp
  void Proceed(bool ok) override;

 private:

  void HandleOp() override;

  // The means to get back to the client.
  ServerAsyncReaderWriter<OpReply, Op>  rw_;

  // Let's implement a tiny state machine with the following states.
  enum class BidiStatus { READ = 1, WRITE = 2, CONNECT = 3, DONE = 4, FINISH = 5 };
  BidiStatus status_;

  std::mutex   m_mutex;

  std::atomic<uint64_t> op_counter_{0};
  std::atomic<uint64_t> batch_counter_{0};
  int reply_counter_{0};

  time_point<high_resolution_clock> start_time_;
  time_point<high_resolution_clock> end_time_;
  time_point<high_resolution_clock> batch_start_time_;
  time_point<high_resolution_clock> batch_end_time_;
};

class ServerImpl final {
  public:
  ServerImpl(const std::string& server_addr, rocksdb::DB* db, SyncServiceImpl* service);

  ~ServerImpl();

  // There is no shutdown handling in this code.
  void Run(int g_thread_num, int g_pool, int g_cq_num);

 private:

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_idx);

  std::shared_ptr<Channel> channel_ = nullptr;
  std::vector<std::unique_ptr<ServerCompletionQueue>>  m_cq;
  SyncServiceImpl* service_;
  std::unique_ptr<Server> server_;
  const std::string& server_addr_;
  ServerBuilder builder_;
  rocksdb::DB* db_;
};

void RunAsyncServer(rocksdb::DB* db, const std::string& server_addr);