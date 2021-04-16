#include "sync_kvstore_client.h"
#include <bits/stdc++.h> 
#include <chrono>
#include <cmath>
#include <random>
#include <bitset>
#include "util.h"
using namespace std;
using std::chrono::high_resolution_clock;

class KvStoreClient{
    public:
      KvStoreClient(std::shared_ptr<SyncKvStoreClient> client, size_t kv_size)
        :client_(client), kv_size_(kv_size){
        }

      ~KvStoreClient(){
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
        random_device rd;   
        mt19937_64 gen(rd());
        unsigned long n = 1024;
        zipf_table_distribution<unsigned long, double> zipf(n);
        default_random_engine eng;
        uniform_int_distribution<int> distr(0, 10000); 

        cout << " zif table initialized \n";
        vector<pair<string, string>> kvs;
        for(int i = 0; i < num_of_kvs; i++){
          string rand_key = bitset<64>(zipf(gen)).to_string();
          string rand_val = bitset<32>(distr(eng)).to_string();
          rand_key.append(bitset<32>(distr(eng)).to_string());
          // cout << " rand key "  << rand_key << " size : " << rand_key.size() << endl;

          rand_key.append(kv_size_ - rand_key.size(), '0');
          rand_val.append(kv_size_ - rand_val.size(), '0');
          kvs.emplace_back(rand_key, rand_val);
        }
        client_->Put(kvs);
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
        // input.insert(0, kv_size_ - input.length() - prefix_len_, '0');
        // input.insert(0, "key");
        // Op request;
        // op_counter_++;
        // request.set_id(op_counter_.load());
        // request.set_type(Op::GET);
        // request.set_key(std::move(input));
        // client_->AddRequests(request);
        // client_->AsyncDoOps();
      }
    }
        
    private:
      std::shared_ptr<SyncKvStoreClient> client_;
      //size of a key and value
      size_t kv_size_;

      // std::default_random_engine gen_;
      // std::random_device rd;   
      // std::mt19937_64 gen_(rd());
      // unsigned long n = 100;
      // zipf_table_distribution<unsigned long, double> zipf_(n);
};


int main(int argc, char** argv){

  if(argc != 2){
    std::cout << "Usage : ./program target_address \n";
    return 0;
  }

  int batch_size = 1000;
 // client used to send kv to ptimary
  std::shared_ptr<SyncKvStoreClient> sync_kv_client = std::make_shared<SyncKvStoreClient>(
    grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()), batch_size);
 
  KvStoreClient client(sync_kv_client, static_cast<size_t>(1024));
  cout << "size of kv pairs : " << 1024 << " bytes" << endl;

  int input;
  while (true)
  {
    cout << "Options : \n" << "Enter 1 to exit the program \n" << "Enter 2 to do put op\n"
              <<"Enter 3 to do getByKey op\n:";
    cin >> input;
    switch (input)
    {
    case 1:
      return 0;
      break;
    
    case 2:
      client.Put();
      break;
    
    case 3:
      client.GetByKey();
      break;

    default:
      break;
    }
  }

  return 0;
}