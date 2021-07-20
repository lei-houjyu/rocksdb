#include "rubble_async_server.h"

static std::atomic<int> counter{0};
static std::unordered_map<std::thread::id, int> map;


// CallData for Bidirectional streaming rpc 
// Take in the "service" instance (in this case representing an asynchronous
// server) and the completion queue "cq" used for asynchronous communication
// with the gRPC runtime.
CallDataBidi::CallDataBidi(SyncServiceImpl* service, 
                ServerCompletionQueue* cq, 
                rocksdb::DB* db,
                std::shared_ptr<Channel> channel)
   :CallDataBase(service, cq, db, channel),rw_(&ctx_){
    // Invoke the serving logic right away.

    status_ = BidiStatus::CONNECT;

    ctx_.AsyncNotifyWhenDone((void*)this);

    // As part of the initial CREATE state, we *request* that the system
    // start processing DoOp requests. In this request, "this" acts are
    // the tag uniquely identifying the request (so that different CallData
    // instances can serve different requests concurrently), in this case
    // the memory address of this CallData instance.
    service_->RequestDoOp(&ctx_, &rw_, cq_, cq_, (void*)this);
    db_options_ = ((rocksdb::DBImpl*)db_)->TEST_GetVersionSet()->db_options(); 
  }


  // async version of DoOp
  void CallDataBidi::Proceed(bool ok) {

    std::unique_lock<std::mutex> _wlock(this->m_mutex);

    switch (status_) {
    case BidiStatus::READ:
        // std::cout << "I'm at READ state ! \n";
        //Meaning client said it wants to end the stream either by a 'writedone' or 'finish' call.
        if (!ok) {
            std::cout << "------server is tail: " << db_options_->is_tail << "\n"; 
            std::cout << "thread:" << map[std::this_thread::get_id()] << " tag:" << this << " CQ returned false." << std::endl;
            Status _st(StatusCode::OUT_OF_RANGE,"test error msg");
            // rw_.Write(reply_, (void*)this);
            rw_.Finish(_st,(void*)this);
            status_ = BidiStatus::DONE;
            std::cout << "thread:" << map[std::this_thread::get_id()] << " tag:" << this << " after call Finish(), cancelled:" << this->ctx_.IsCancelled() << std::endl;
            break;
        }

        // std::cout << "thread:" << std::setw(2) << map[std::this_thread::get_id()] << " Received a new " << Op_OpType_Name(request_.type()) << " op witk key : " << request_.key() << std::endl;
        // Handle a db operation
        // std::cout << "entering HandleOp\n";
        HandleOp();
        counter.fetch_add(1);
        std::cout  << " counter : " << counter.load() << std::endl;
        assert(request_.ops_size());
        /* chain replication */
        // Forward the request to the downstream node in the chain if it's not a tail node
        if(!db_options_->is_tail){
          if (forwarder_ == nullptr) {
            std::cout << "init the forwarder" << "\n";
            forwarder_ = std::make_shared<Forwarder>(channel_);
          }
          // forwarder_->Forward(request_);
            // request_.clear_ops();
          // std::cout << "thread: " << map[std::this_thread::get_id()] << " Forwarded " << op_counter_ << " ops" << std::endl;
        }else {
          // tail node should be responsible for sending the reply back to replicator
          // use the sync stream to write the reply back
          if (reply_client_ == nullptr) {
            std::cout << "init the reply client" << "\n";
            reply_client_ = std::make_shared<ReplyClient>(channel_);
          }
          reply_client_->SendReply(reply_);
          // reply_.clear_replies();
        }
        rw_.Read(&request_, (void*)this);
        status_ = BidiStatus::READ;
        // rw_.Write(reply_, (void*)this);
        // status_ = BidiStatus::WRITE;
        break;

    case BidiStatus::WRITE:
        // std::cout << "I'm at WRITE state ! \n";
        // std::cout << "thread:" << map[std::this_thread::get_id()] << " tag:" << this << " Get For key : " << request_.key() << " , status : " << reply_.status() << std::endl;
        // For a get request, return a reply back to the client
        rw_.Read(&request_, (void*)this);

        status_ = BidiStatus::READ;
        break;

    case BidiStatus::CONNECT:
        std::cout << "thread:" << std::setw(2) << map[std::this_thread::get_id()] << " tag:" << this << " connected:" << std::endl;
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new CallDataBidi(service_, cq_, db_, channel_);
        rw_.Read(&request_, (void*)this);
        // request_.set_type(Op::PUT);
        // std::cout << "thread:" << std::setw(2) << map[std::this_thread::get_id()] << " Received a new " << Op_OpType_Name(request_.type()) << " op witk key : " << request_.key() << std::endl;
        status_ = BidiStatus::READ;
        break;

    case BidiStatus::DONE:
        std::cout << "thread:" << std::this_thread::get_id() << " tag:" << this
                << " Server done, cancelled:" << this->ctx_.IsCancelled() << std::endl;
        status_ = BidiStatus::FINISH;
        break;

    case BidiStatus::FINISH:
        std::cout << "thread:" << map[std::this_thread::get_id()] <<  "tag:" << this << " Server finish, cancelled:" << this->ctx_.IsCancelled() << std::endl;
        _wlock.unlock();
        delete this;
        break;

    default:
        std::cerr << "Unexpected tag " << int(status_) << std::endl;
        assert(false);
    }
}


void CallDataBidi::HandleOp(){
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
    op_counter_ += 1;

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
        batch_counter_++;
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
  


ServerImpl::ServerImpl(const std::string& server_addr, rocksdb::DB* db, SyncServiceImpl* service)
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
            new CallDataBidi(service_, m_cq[_cq_idx].get(), db_, channel_);
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

      CallDataBase* _p_ins = (CallDataBase*)tag;
      _p_ins->Proceed(ok);
    }
}

void RunAsyncServer(rocksdb::DB* db, const std::string& server_addr){
  
  SyncServiceImpl service(db);

  int g_thread_num = 16;
  int g_cq_num = 8;
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

