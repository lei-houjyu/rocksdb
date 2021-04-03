#include "kvstore_client.h"
#include <bits/stdc++.h> 
#include <chrono>
#include <cmath>
using namespace std;
using std::chrono::high_resolution_clock;

class AsyncKvStoreClient{
    public:
      AsyncKvStoreClient(std::shared_ptr<KvStoreClient> client, size_t kv_size)
        :client_(client), kv_size_(kv_size){
          prefix_len_ = std::string("key").length();
        }

      ~AsyncKvStoreClient(){

      }

    void Put(){
      string input;
      int num_of_kvs;
      while(true){
        cout << "Enter the number of kv pairs to put or Enter \"quit\" to go back to options\n:";
        cin >> input;
        if(input == "quit"){
          break;
        }
        num_of_kvs = stoi(input);
        for(int i = 0; i < num_of_kvs; i++){
          std::string kv_num = std::to_string(op_counter_.load());
          // kv_num.insert(0, kv_size_ - kv_num.length() - std::string("key").length(), '0');
          Op request;
          op_counter_++;
          request.set_id(op_counter_.load());
          request.set_type(Op::PUT);
          std::string key("key" + std::string(kv_size_ - prefix_len_ - kv_num.length(), '0') + kv_num);
          std::string val("val" + std::string(kv_size_ - prefix_len_ - kv_num.length(), '0') + kv_num);
          // std::cout << "kv pair size : " << key.size() + key.size() << std::endl;
          request.set_key(std::move(key));
          request.set_value(std::move(val));
          client_->AddRequests(request);
        }
        client_->SetStartTime(high_resolution_clock::now());
        client_->AsyncDoOps();
      }
    }

    void GetByKey(){
      while(true){
        cout << "Enter the key of kv pair you want to get or Enter \"quit\" to go back to options \n:";
        string input;
        cin >> input;
        if(input == "quit"){
          break;
        }
        input.insert(0, kv_size_ - input.length() - prefix_len_, '0');
        input.insert(0, "key");
        Op request;
        op_counter_++;
        request.set_id(op_counter_.load());
        request.set_type(Op::GET);
        request.set_key(std::move(input));
        client_->AddRequests(request);
        client_->AsyncDoOps();
      }
    }

    void GetByKeyRange(){
      while(true){
        cout << "Enter the key range you want to get or Enter \"quit\" to go back to options \n:";
        cout << "start key :";
        string key;
        cin >> key;
        if(key == "quit"){
          break;
        }
        int start_key = stoi(key);
        int end_key;
        cout << "end key :";
        cin >> end_key;

        Op request;
        for(int i = start_key ; i <= end_key; i++){
          int num_len = to_string(i).length();
          op_counter_++;
          request.set_id(op_counter_.load());
          request.set_type(Op::GET);
          std::string key = "key" + std::string(kv_size_ - std::string("key").length() - std::to_string(i).length(),'0') + std::to_string(i);
          request.set_key(std::move(key));
          client_->AddRequests(request);
        }
        client_->SetStartTime(high_resolution_clock::now());
        client_->AsyncDoOps();
      }
    }
        
    private:
      std::shared_ptr<KvStoreClient> client_;
      atomic<uint64_t> op_counter_{0};
      //size of a key and value
      size_t kv_size_;
      int prefix_len_;
};


int main(int argc, char** argv){

  if(argc != 2){
    std::cout << "Usage : ./program target_address \n";
    return 0;
  }
// client used to send kv to ptimary
  std::shared_ptr<KvStoreClient> client1 = std::make_shared<KvStoreClient>(
    grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()));
 
  client1->SetKvClient(true);

  AsyncKvStoreClient async_client(client1, 512);
  cout << "size of kv pairs : " << 1024 << " bytes" << endl;

  int input;
  while (true)
  {
    cout << "Options : \n" << "Enter 1 to exit the program \n" << "Enter 2 to do put op\n"
              <<"Enter 3 to do getByKey op\n" << "Enter 4 to do getByKeyRange op\n:";
    cin >> input;
    switch (input)
    {
    case 1:
      return 0;
      break;
    
    case 2:
      async_client.Put();
      break;
    
    case 3:
      async_client.GetByKey();
      break;
    case 4:
      async_client.GetByKeyRange();
      break;

    default:
      break;
    }
  }

  return 0;
}