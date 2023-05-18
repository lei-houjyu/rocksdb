#include "sync_client.h"
#include "util/coding.h"

#include <chrono>
#include <set>
#include <nlohmann/json.hpp>
using grpc::Status;
using json = nlohmann::json;

SyncClient::SyncClient(std::shared_ptr<Channel> channel)
    :stub_(RubbleKvStoreService::NewStub(channel)){
        // grpc_thread_.reset(new std::thread(std::bind(&SyncClient::AsyncCompleteRpc, this)));
        // stream_ = stub_->AsyncSync(&context_, &cq_, reinterpret_cast<void*>(Type::CONNECT));
        sync_stream_ = stub_->Sync(&context_);
    }; 

SyncClient::~SyncClient(){
    std::cout << "Shutting down client...." << std::endl;
    // cq_.Shutdown();
    // grpc_thread_->join();
}

void SyncClient::Sync(const std::string& args, int rid) {
    if (need_recovery) {
        std::cout << "[Sync] need recovery, simply return\n";
        return;
    }
    SyncRequest request;
    request.set_args(args);
    request.set_rid(rid);
    Sync(request);
}

void SyncClient::Sync(const SyncRequest& request) {
    bool s = sync_stream_->Write(request);
    if (!s) {
        std::cout << "[Sync] fail!\n";
        need_recovery = true;
    }
}

// read a reply back for a sync request
void SyncClient::GetSyncReply(SyncReply *reply) {
    // The tag is the link between our thread (main thread) and the completion
    // queue thread. The tag allows the completion queue to fan off
    // notification handlers for the specified read/write requests as they
    // are being processed by gRPC.

    if (!sync_stream_->Read(reply)) {
        sync_stream_->WritesDone();
        Status s = sync_stream_->Finish();
        std::cout << "Sync fail!"
                << " msg: " << s.error_message() 
                << " detail: " << s.error_details() 
                << " debug: " << context_.debug_error_string() << std::endl;
        assert(false);
    }
    // stream_->Read(&reply_, reinterpret_cast<void*>(Type::READ));
}

bool SyncClient::CheckReply(const SyncReply& reply){
    assert(reply.message().compare("ok") == 0);
    return true;
    // auto j_reply = json::parse(reply.message());
    // std::cout << "[Sync Reply] : " << j_reply.dump(4) << std::endl;
    // auto reply_id = j_reply["Id"].get<uint64_t>();

    // auto j_message = json::parse(j_reply["Message"].get<std::string>());
    // if(j_message["Status"].get<std::string>().compare("Ok") == 0){
    //     // succeeds
    //     return true;
    // }else{
    //     // Sync rpc Failed for some reason
    //     std::cout << "Sync rpc Failed : " << j_message["Reason"].get<std::string>() << std::endl;
    //     return false;
    // }
}

// Loop while listening for completed responses.
// Prints out the response from the server.
void SyncClient::AsyncCompleteRpc() {
    void* got_tag;
    bool ok = false;
    json j_reply;
    json j_message;
    // uint64_t reply_id;
    std::set<uint64_t> deleted_files;

    // Block until the next result is available in the completion queue "cq".
    while (cq_.Next(&got_tag, &ok)) {
    // Verify that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
      GPR_ASSERT(ok);

      switch (static_cast<Type>(reinterpret_cast<long>(got_tag))) {
        case Type::READ:
            // update the sst bit map in the callback function
            deleted_files.clear();
            j_reply = json::parse(reply_.message());
            std::cout << "[Reply] : " << j_reply.dump(4) << std::endl;
            // reply_id = j_reply["Id"].get<uint64_t>();
            j_message = json::parse(j_reply["Message"].get<std::string>());
            if(j_message["Status"].get<std::string>().compare("Ok") == 0){
                if(j_message["Type"].get<std::string>().compare("Full") == 0){
                    for(const auto& file_num : j_message["Deleted"].get<std::vector<uint64_t>>()){
                        deleted_files.insert(file_num);
                    }
                    rocksdb::FreeSstSlot(deleted_files);
                }
            }else{
                // Sync rpc Failed for some reason
                std::cout << "Sync rpc Failed : " << j_message["Reason"].get<std::string>() << std::endl;
                assert(false); 
            }

            // ready_.store(true);
            // std::cout << "notifying\n";
            // cv_.notify_one();
            break;
        case Type::WRITE:
            // GetSyncReply();
            ready_.store(true);
            std::cout << "notifying\n";
            cv_.notify_one();
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