#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "rubble_kv_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientAsyncResponseReader;
using grpc::CompletionQueue;

using rubble::RubbleKvStoreService;
using rubble::Op;
using rubble::OpReply;
// using rubble::SingleOp_OpType_Name;


// client class used by the non-tail node in the chain to forward the op to the downstream node.
class Forwarder{
  public:
    Forwarder(std::shared_ptr<Channel> channel)
        : stub_(RubbleKvStoreService::NewStub(channel)) {    
    };

    ~Forwarder(){
    }

    // forward the op to the next node
    void Forward(const Op& op){
        // Call object to store rpc data
        AsyncClientCall* call = new AsyncClientCall;

        // stub_->PrepareAsyncSayHello() creates an RPC object, returning
        // an instance to store in "call" but does not actually start the RPC
        // Because we are using the asynchronous API, we need to hold on to
        // the "call" instance in order to get updates on the ongoing RPC.
        call->response_reader =
            stub_->PrepareAsyncDoOp(&call->context, op, &cq_);

        // StartCall initiates the RPC call
        call->response_reader->StartCall();

        // Request that, upon completion of the RPC, "reply" be updated with the
        // server's response; "status" with the indication of whether the operation
        // was successful. Tag the request with the memory address of the call object.
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
    }

    // Loop while listening for completed responses.
    // Prints out the response from the server.
    OpReply AsyncCompleteRpc() {
        void* got_tag;
        bool ok = false;

        // Block until the next result is available in the completion queue "cq".
        while (cq_.Next(&got_tag, &ok)) {
            // The tag in this example is the memory location of the call object
            AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            GPR_ASSERT(ok);

            if (call->status.ok()) {
                // std::cout << "Greeter received\n";
                return call->reply;
            } else {
                std::cout << "RPC failed" << std::endl;
            }

            // Once we're complete, deallocate the call object.
            delete call;
        }
    }
  
  private:
    // struct for keeping state and data information
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        OpReply reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;

        std::unique_ptr<ClientAsyncResponseReader<OpReply>> response_reader;
    };

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<RubbleKvStoreService::Stub> stub_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;
};
