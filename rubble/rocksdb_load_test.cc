// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <thread>
#include <unordered_map>
#include <bitset>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "util.h"
using namespace std;
using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_load";
#endif

class InsertClient{
  public:
    InsertClient(DB* db, vector<pair<string, string>>& kvs)
      :db_(db),kvs_(kvs){
      thread_num_ = std::thread::hardware_concurrency();
      std::cout << "InsertClient initialized, thread num : " << thread_num_ << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    ~InsertClient(){}

  void DoOp(int index, int target){
    auto start_time = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < target; ++i){
      auto kv = kvs_[index + i];
      db_->Put(WriteOptions(), kv.first, kv.second);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::cout << "Thread " << map_[std::this_thread::get_id()] << " process time (micros): " 
              << std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() << std::endl;
  }

  void StartDoOp(){
    std::cout << "Start Processing Op \n";
    assert(kvs_.size() > thread_num_);
    int target = kvs_.size()/ thread_num_;
    std::cout << "requests size : " << kvs_.size() << ", target : " << target << std::endl;

    for(unsigned int i = 0; i < thread_num_; ++i){
      auto new_thread = new std::thread(&InsertClient::DoOp, this, i*target, target);
      threads_.emplace_back(new_thread);
      map_.emplace(new_thread->get_id(), i);
    }

    for(const auto& t: threads_){
      t->join();
    }
  }

  private:
   int target;
   std::vector<std::thread*> threads_;
   int thread_num_;

   std::unordered_map<std::thread::id, int> map_;
   vector<pair<string, string>> kvs_;
   DB* db_;

};

int main() {
  // open DB
  rocksdb::DB* db = GetDBInstance("/tmp/rocksdb_vanila_test","/tmp/rocksdb_sst_dir", "" , "", false, false, false);
  Status s;

  string input;
  int num_of_kvs;
  int kv_size = 1024;
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
    uniform_int_distribution<int> distr(0, 1000); 

    cout << " zif table initialized \n";
    vector<pair<string, string>> kvs;
    for(int i = 0; i < num_of_kvs; i++){
      string rand_key = bitset<64>(zipf(gen)).to_string();
      string rand_val = bitset<32>(distr(eng)).to_string();
      rand_key.append(bitset<32>(distr(eng)).to_string());
      rand_key.append(kv_size - rand_key.size(), '0');
      rand_val.append(kv_size - rand_val.size(), '0');
      kvs.emplace_back(rand_key, rand_val);
    }

    InsertClient client(db, kvs);
    auto start_time = chrono::high_resolution_clock::now();
    client.StartDoOp();
    // for(const auto& kv : kvs){
    //   s = db->Put(WriteOptions() , kv.first, kv.second);
    // }
    auto end_time = chrono::high_resolution_clock::now();
    auto process_time = chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();
    cout << "process " << num_of_kvs << " in " << process_time << " millisecs \n";
  }

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  {
    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value");
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point

  delete db;

  return 0;
}
