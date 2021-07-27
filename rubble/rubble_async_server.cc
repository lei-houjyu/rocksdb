#include "rubble_async_server.h"

static std::atomic<int> counter{0};
static std::unordered_map<std::thread::id, int> map;

// async version of DoOp
void CallData::Proceed() {
  switch (status_) {
    case CallStatus::CREATE:
      status_ = CallStatus::PROCESS;
      service_->RequestDoOp(&ctx_, &request_, &responder_, cq_, cq_, (void*)this);
      db_options_ = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->db_options();
      break;
    case CallStatus::PROCESS:
      // Spawn a new CallData instance to serve new clients while we process
      // the one for this CallData. The instance will deallocate itself as
      // part of its FINISH state.
      new CallData(service_, cq_, db_, channel_);

      // The actual processing.
      HandleOp();
      counter.fetch_add(request_.ops_size());
      std::cout  << "counter : " << counter.load() << std::endl;
      
      /* chain replication */
      // Forward the request to the downstream node in the chain if it's not a tail node
      if(!db_options_->is_tail){
        if (forwarder_ == nullptr) {
          std::cout << "init the forwarder" << "\n";
          forwarder_ = std::make_shared<Forwarder>(channel_);
        }
        // forwarder_->AsyncForward(request_);
        // std::cout << "thread: " << map[std::this_thread::get_id()] << " Forwarded " << op_counter_ << " ops" << std::endl;
      }else {
        if (reply_client_ == nullptr) {
          // std::cout << "init the reply client" << "\n";
          reply_client_ = std::make_shared<ReplyClient>(channel_);
        }
        // reply_client_->SendReply(reply_);
      }
      status_ = CallStatus::FINISH;
      // responder_.Finish(reply_, Status::OK, this);
      break;
    case CallStatus::FINISH:
      break;
    default:
      std::cerr << "Should not reach here\n";
      assert(false);
  }
}


void CallData::HandleOp(){
    std::string value;
    // if(!op_counter_.load()){
    //   start_time_ = high_resolution_clock::now();
    // }

    // if(op_counter_.load() && op_counter_.load()%100000 == 0 ){
    //   std::cout << "opcount: " << op_counter_.load() << "\n";
    //   end_time_ = high_resolution_clock::now();
    //   auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_);
    //   std::cout << "Throughput : handled 100000 ops in " << std::to_string(millisecs.count()) << " millisecs\n";
    //   start_time_ = end_time_;
    // }
    
    // std::cout << "handling a " << request_.type() <<" " <<  request_.id() << " op...\n";
    // ASSUME that each batch is with the same type of operation
    assert(request_.ops_size() > 0);

    SingleOpReply* reply;
    reply_.clear_replies();
    // std::cout << "thread: " << map[std::this_thread::get_id()] << " op_counter: " << op_counter_ << std::endl;
    //     <<  " first key in batch: " << request_.ops(0).key() << " size: " << request_.ops_size() << "\n";
    switch (request_.ops(0).type())
    {
      case rubble::GET:
        for(const auto& request: request_.ops()) {
          reply = reply_.add_replies();
          reply->set_id(request.id());
          s_ = db_->Get(rocksdb::ReadOptions(), request.key(), &value);
          reply->set_key(request.key());
          reply->set_type(rubble::GET);
          reply->set_status(s_.ToString());
          if(s_.ok()){
            reply->set_ok(true);
            reply->set_value(value);
          }else{
            reply->set_ok(false);
            reply->set_status(s_.ToString());
          }
        }
        break;
      case rubble::PUT:
        // batch_start_time_ = high_resolution_clock::now();
        for(const auto& request: request_.ops()) {
          s_ = db_->Put(rocksdb::WriteOptions(), request.key(), request.value());
         if(!s_.ok()){
	        std::cout << "Put Failed : " << s_.ToString() << std::endl;
	      }
          // std::cout << "Put ok\n";
          // return to replicator if tail
          if(db_options_->is_tail){
            reply = reply_.add_replies();
            reply->set_id(request.id());
            reply->set_type(rubble::PUT);
            reply->set_key(request.key());
            if(s_.ok()){
              // std::cout << "Put : (" << request.key() /* << " ," << request_.value() */ << ")\n"; 
              reply->set_ok(true);
            }else{
              std::cout << "Put Failed : " << s_.ToString() << std::endl;
              reply->set_ok(false);
              reply->set_status(s_.ToString());
            }
          }
        }
        // batch_end_time_ = high_resolution_clock::now();
        // std::cout << "thread " << map[std::this_thread::get_id()] << " processd batch " << batch_counter_.load() 
        //           << " in " << std::to_string(duration_cast<std::chrono::milliseconds>(batch_end_time_ - batch_start_time_).count())
        //           << " millisecs , size : " << request_.ops_size() << std::endl;

        break;
      case rubble::DELETE:
        //TODO
        break;

      case rubble::UPDATE:
        // std::cout << "in UPDATE " << request_.ops(0).key() << "\n"; 
        for(const auto& request: request_.ops()) {
          reply = reply_.add_replies();
          reply->set_id(request.id());
          s_ = db_->Get(rocksdb::ReadOptions(), request.key(), &value);
          s_ = db_->Put(rocksdb::WriteOptions(), request.key(), request.value());
          reply->set_key(request.key());
          reply->set_type(rubble::UPDATE);
          reply->set_status(s_.ToString());
          if(s_.ok()){
            reply->set_ok(true);
            reply->set_value(value);
          }else{
            reply->set_ok(false);
          }
        }
        break;

      default:
        std::cerr << "Unsupported Operation \n";
        break;
    }
}
  


ServerImpl::ServerImpl(const std::string& server_addr, rocksdb::DB* db, RubbleKvStoreService::AsyncService* service)
   :server_addr_(server_addr), db_(db), service_(service){
        auto db_options = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->db_options(); 
        std::cout << "target address : " << db_options->target_address << std::endl;
        if(db_options->target_address != ""){
        channel_ = grpc::CreateChannel(db_options->target_address, grpc::InsecureChannelCredentials());
        assert(channel_ != nullptr);
    }
}

ServerImpl::~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (const auto& _cq : m_cq)
        _cq->Shutdown();
}

// There is no shutdown handling in this code.
void ServerImpl::Run(int g_thread_num, int g_pool, int g_cq_num) {

    builder_.AddListeningPort(server_addr_, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an asynchronous DoOp service and synchronous Sync service
    builder_.RegisterService(service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
   
    for (int i = 0; i < g_cq_num; ++i) {
        //cq_ = builder.AddCompletionQueue();
        m_cq.emplace_back(builder_.AddCompletionQueue());
    }

    // Finally assemble the server.
    server_ = builder_.BuildAndStart();
  
    // Proceed to the server's main loop.
    std::vector<std::thread*> _vec_threads;

    for (int i = 0; i < g_thread_num; ++i) {
        int _cq_idx = i % g_cq_num;
        for (int j = 0; j < g_pool; ++j) {
            new CallData(service_, m_cq[_cq_idx].get(), db_, channel_);
        }
        auto new_thread =  new std::thread(&ServerImpl::HandleRpcs, this, _cq_idx);
        map[new_thread->get_id()] = i;
        _vec_threads.emplace_back(new_thread);
    }

    std::cout << g_thread_num << " working aysnc threads spawned" << std::endl;

    for (const auto& _t : _vec_threads)
        _t->join();
}


// This can be run in multiple threads if needed.
void  ServerImpl::HandleRpcs(int cq_idx) {
    // Spawn a new CallDataUnary instance to serve new clients.
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallDataUnary instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(m_cq[cq_idx]->Next(&tag, &ok));
      GPR_ASSERT(ok);

      CallData* _p_ins = (CallData*)tag;
      _p_ins->Proceed();
    }
}

void RunAsyncServer(rocksdb::DB* db, const std::string& server_addr){
  
  RubbleKvStoreService::AsyncService service;

  int g_thread_num = 16;
  int g_cq_num = 16;
  int g_pool = 1;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  // ServerBuilder builder;

  // builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
  std::cout << "Server listening on " << server_addr << std::endl;
  // builder.RegisterService(&service);
  ServerImpl server_impl(server_addr, db, &service);
  server_impl.Run(g_thread_num, g_pool, g_cq_num);
}

