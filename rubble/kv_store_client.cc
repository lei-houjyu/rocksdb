#include "kvstore_client.h"

int main(){

// client used to send kv to ptimary
  KvStoreClient client1(grpc::CreateChannel(
    "localhost:50051", grpc::InsecureChannelCredentials()));
// sending kv to the secondary
  KvStoreClient client2(grpc::CreateChannel(
    "localhost:50050", grpc::InsecureChannelCredentials()));

  grpc::Status s;
  std::vector<std::pair<std::string, std::string>> kvs;

  // do 500000 put ops
  // Got an IO error when hitting about 734000 :  IO error: While open a file for appending: /tmp/rocksdb_secondary_test/001006.log: Too many open files 
  int num_of_kv = 500000;
  for (int i = 0; i < num_of_kv; i++){
      //sending the kv pairs to both primary and secondary server
      kvs.emplace_back("key" + std::to_string(i), "val" + std::to_string(i));
      // client1.Put(std::pair<std::string, std::string>("key" + std::to_string(i), "val" + std::to_string(i)));
      // assert(s.ok());
      // client2.Put(std::pair<std::string, std::string>("key" + std::to_string(i), "val" + std::to_string(i)));
      // assert(s.ok());
  }
  
  client1.Put(kvs);
  
  std::vector<std::string> keys;
  for(int i = 0; i <num_of_kv; i++){
    keys.emplace_back("key" + std::to_string(i));
  }

  std::vector<std::string> vals;
  client1.Get(keys, vals);
  for(int i = 0 ; i < vals.size(); i++){
    assert(vals[i] == ("val" + std::to_string(i)));
  }

  vals.erase(vals.begin(), vals.end());
  client2.Get(keys, vals);
  for(int i = 0 ; i < vals.size(); i++){
    assert(vals[i] == ("val" + std::to_string(i)));
  }

  return 0;
}