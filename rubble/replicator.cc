#include <iostream>
#include <iomanip>
#include <string> 
#include <memory>
#include <thread>
#include <bitset>
#include <unordered_map>
#include <condition_variable>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/alarm.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"
#include "kvstore_client.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;
using rubble::Reply;
using rubble::Op_OpType_Name;

using std::chrono::time_point;
using std::chrono::high_resolution_clock;

class Replicator final : public  RubbleKvStoreService::Service {
  public:
    explicit Replicator(const std::vector<std::string>& shards)
     :num_of_shards_(shards.size()) {
          for(const auto& shard: shards){
              forwarders_.emplace_back(std::make_shared<KvStoreClient>(grpc::CreateChannel(shard, grpc::InsecureChannelCredentials())));
          }

    };

    ~Replicator() {}

    class Task{
      public:
        Task(std::thread::id id,
                    std::condition_variable* cv,
                    bool& ready)
          :id_(id), cv_(cv), ready_(ready){
            }

        ~Task(){}

        void SetReply(const OpReply& reply){
            reply_ = reply;
        }

        OpReply GetReply(){
            return reply_;
        }
        void Notify(){
            (*cv_).notify_one();
        }

        void SetReady(){
            ready_ = true;
        }

    private:
        OpReply reply_;
        bool ready_ = false;
        // std::mutex* mu_;
        std::thread::id id_;
        std::condition_variable* cv_;
    };

    // called by the kvstore client
    // replicator doesn't actually perform an op, but just forward it to one shard
    Status DoOp(ServerContext* context, 
              ServerReaderWriter<OpReply, Op>* stream) override {
        std::string value;
        if(!op_counter_.load()){
            start_time_ = high_resolution_clock::now();
        }

        if(op_counter_.load() && op_counter_.load()%100000 == 0){
            end_time_ = high_resolution_clock::now();
            auto millisecs = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_);
            std::cout << "Throughput : handled 100000  in " << millisecs.count() << " milisecs\n";
            start_time_ = end_time_;
        }

        op_counter_.fetch_add(1);
        Op request;
        while (stream->Read(&request)){
            std::string key = request.key();
            // take the last two bits in string cause num_of_shards_ is supposed to be 3 in our setting
            int shard_idx = ((int)(std::bitset<8>(key[key.length() - 1]) & mask_).to_ulong())% num_of_shards_;
            uint64_t id = request.id();
            // forward the op to the corrensponding shard
            forwarders_[shard_idx]->ForwardOp(request);
            std::mutex mu;
            std::condition_variable cv;
            bool ready;
            Task task(std::this_thread::get_id(), &cv, ready);
            map_.emplace(id, &task);
            {
                std::unique_lock<std::mutex> lk{mu};
                // wait until got notified (when the reply returned from the tail node)
                cv.wait(lk, [&](){return ready;});
            }
            
            stream->Write(task.GetReply());
        }
        return Status::OK;
    }

    // used by the tail node in the chain to send the true reply back to the replicator
    // replicator is then responsible for sending this reply back to the client
    Status SendReply(ServerContext* context, 
              ServerReaderWriter<Reply, OpReply>* stream) override {
        
        OpReply reply;
        while(stream->Read(&reply)){
            uint64_t id = reply.id();
            auto it = map_.find(id);
            assert(it != map_.end());
            Task* task = it->second;
            task->SetReply(reply);
            task->SetReady();
            task->Notify();
            map_.erase(it);
        }
        return Status::OK;
    }

  private:

    // a db op request we get from the client
    Op request_;
    //reply we get back to the client for a db op
    OpReply reply_;

    std::atomic<uint64_t> op_counter_{0};
    time_point<high_resolution_clock> start_time_;
    time_point<high_resolution_clock> end_time_;
    
    int num_of_shards_;
    // used for forwarding the requests to the corresponding shards
    std::vector<std::shared_ptr<KvStoreClient>> forwarders_;

    std::bitset<8> mask_ {std::string{"00000011"}};

    // keep a mapping between the request and the thread for handing it
    std::unordered_map<uint64_t, Task*> map_;
};

int main(int argc, char** argv) {
  
  if(argc <= 2){
      std::cout << " usage : ./program server_address shards' primary instance's address(pass at least one) \n";
      return 0;
  }

  std::string server_addr = argv[1];
  std::vector<std::string> shards;
  for(int i = 2 ; i < argc; i++){   
      shards.emplace_back(argv[i]);
  }

  Replicator service(shards);
  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;

  builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
  std::cout << "Server listening on " << server_addr << std::endl;
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  server->Wait();
}