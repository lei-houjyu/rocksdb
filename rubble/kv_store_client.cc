#include "kvstore_client.h"
#include <bits/stdc++.h> 
#include <chrono>
#include <cmath>
using namespace std;
using std::chrono::high_resolution_clock;

int numDigits(int x)
{
    if (x >= 10000) {
        if (x >= 10000000) {
            if (x >= 100000000) {
                if (x >= 1000000000)
                    return 10;
                return 9;
            }
            return 8;
        }
        if (x >= 100000) {
            if (x >= 1000000)
                return 7;
            return 6;
        }
        return 5;
    }
    if (x >= 100) {
        if (x >= 1000)
            return 4;
        return 3;
    }
    if (x >= 10)
        return 2;
    return 1;
}

void Put(KvStoreClient& client){
  string input;
  int num_of_kvs;
  while(true){
    cout << "Enter the number of kv pairs to put or Enter \"quit\" to go back to options\n:";
    cin >> input;
    if(input == "quit"){
      break;
    }
    num_of_kvs = stoi(input);

    int num_of_digits = numDigits(num_of_kvs);
    int current_num_of_digits = 0;
    string prefix;
    vector<pair<string, string>> kvs;
    while(current_num_of_digits < num_of_digits - 1){
      prefix = string(7 - current_num_of_digits , '0');
      int start = 1;
      for(int i = 0 ; i < current_num_of_digits; i++){
        start *= 10;
      }
      int end = start*10;
  
      for(int i = start ; i < end ; i++){
        kvs.emplace_back("key" + prefix + to_string(i), "val" + prefix + to_string(i));
      }
      current_num_of_digits++;
    }

    prefix = string(8 - num_of_digits, '0');
    int start = 1;
    for(int i = 0 ; i < num_of_digits - 1; i++){
      start *= 10;
    }

    for(int i = start; i <= num_of_kvs; i++){
      kvs.emplace_back("key" + prefix + to_string(i), "val" + prefix + to_string(i));
    }
    client.SyncDoPuts(kvs);
  }
}

void GetByKey(KvStoreClient& client){
  while(true){
    cout << "Enter the key of kv pair you want to get or Enter \"quit\" to go back to options \n:";
    string key;
    cin >> key;
    if(key == "quit"){
      break;
    }
    int num_len = key.length();
    vector<string> keys;
    keys.emplace_back("key" + string("00000000").replace(8 - num_len, num_len, key));
    client.SyncDoGets(keys);
  }
}

void GetByKeyRange(KvStoreClient& client){
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

    vector<string> keys;
    for(int i = start_key ; i <= end_key; i++){
      int num_len = to_string(i).length();
      keys.emplace_back("key" + string("00000000").replace(8 - num_len, num_len, to_string(i)));
    }
    client.SyncDoGets(keys);
  }
}

int main(){

// client used to send kv to ptimary
  KvStoreClient client1(grpc::CreateChannel(
    "localhost:50051", grpc::InsecureChannelCredentials()));

  int size_of_kv_pairs = sizeof("key00000000") + sizeof("val00000000");
  cout << "size of kv pairs : " << size_of_kv_pairs << "bytes" << endl;

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
      Put(client1);
      break;
    
    case 3:
      GetByKey(client1);
      break;
    case 4:
      GetByKeyRange(client1);
      break;

    default:
      break;
    }
  }

  return 0;
}