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
#include "port/port_posix.h"
#include "port/port.h"
#include "db/version_edit.h"
#include "db/db_impl/db_impl.h"
#include "util/aligned_buffer.h"
#include "file/read_write_util.h"
#include "logging/event_logger.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerReaderWriter;
using grpc::ServerAsyncResponseWriter;
using grpc::Status;
using grpc::StatusCode;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;
using rubble::SingleOp;
using rubble::SingleOpReply;
using rubble::OpType_Name;

using std::chrono::time_point;
using std::chrono::high_resolution_clock;

class CallData {
public:
  CallData(RubbleKvStoreService::AsyncService* service, 
           ServerCompletionQueue* cq, 
           rocksdb::DB* db, 
           std::shared_ptr<Channel> channel)
    :service_(service), cq_(cq), db_(db), channel_(channel), responder_(&ctx_), status_(CREATE) {
      Proceed();
    }

  void Proceed();

  void HandleOp();

private:

  // db instance
  rocksdb::DB* db_;

  // status of the db after performing an operation.
  rocksdb::Status s_;

  const rocksdb::ImmutableDBOptions* db_options_;

  std::shared_ptr<Channel> channel_ = nullptr;
  std::shared_ptr<Forwarder> forwarder_ = nullptr;

  // The means of communication with the gRPC runtime for an asynchronous
  // server.
  RubbleKvStoreService::AsyncService* service_;
  // The producer-consumer queue where for asynchronous server notifications.
  ServerCompletionQueue* cq_;

  // Context for the rpc, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  ServerContext ctx_;

  // The means to get back to the client.
  ServerAsyncResponseWriter<OpReply> responder_;

  // What we get from the client.
  Op request_;
  // What we send back to the client.
  OpReply reply_;

  // Let's implement a tiny state machine with the following states.
  enum CallStatus { CREATE, PROCESS, FINISH };
  // The current serving state.
  CallStatus status_;
};

class ServerImpl final {
  public:
  ServerImpl(const std::string& server_addr, rocksdb::DB* db, RubbleKvStoreService::AsyncService* service);

  ~ServerImpl();

  // There is no shutdown handling in this code.
  void Run(int g_thread_num, int g_pool, int g_cq_num);

 private:

  // This can be run in multiple threads if needed.
  void HandleRpcs(int cq_idx);

  std::shared_ptr<Channel> channel_ = nullptr;
  std::vector<std::unique_ptr<ServerCompletionQueue>>  m_cq;
  RubbleKvStoreService::AsyncService* service_;
  std::unique_ptr<Server> server_;
  const std::string& server_addr_;
  ServerBuilder builder_;
  rocksdb::DB* db_;
};

void RunAsyncServer(rocksdb::DB* db, const std::string& server_addr);