#pragma once

#include <string>
#include <iostream>
#include <cstring>
#include <random>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/options_util.h"
#include "sync_rubble_server.h"

using std::string;
using rocksdb::LoadOptionsFromFile;

const char* ParseCmdPara( char* argv,const char* para) {
   auto p_target = std::strstr(argv,para);
   if (p_target == nullptr) {
      printf("para error argv[%s] should be %s \n",argv,para);
      return nullptr;
   }
   p_target += std::strlen(para);
   return p_target;
}

/**
 * Example usage:
 *
 *    std::random_device rd;
 *    std::mt19937 gen(rd());
 *    zipf_table_distribution<> zipf(300);
 *
 *    for (int i = 0; i < 100; i++)
 *        printf("draw %d %d\n", i, zipf(gen));
 */
template<class IntType = unsigned long, class RealType = double>
class zipf_table_distribution
{
   public:
      typedef IntType result_type;

      static_assert(std::numeric_limits<IntType>::is_integer, "");
      static_assert(!std::numeric_limits<RealType>::is_integer, "");

      /// zipf_table_distribution(N, s)
      /// Zipf distribution for `N` items, in the range `[1,N]` inclusive.
      /// The distribution follows the power-law 1/n^s with exponent `s`.
      /// This uses a table-lookup, and thus provides values more
      /// quickly than zipf_distribution. However, the table can take
      /// up a considerable amount of RAM, and initializing this table
      /// can consume significant time.
      zipf_table_distribution(const IntType n,
                              const RealType q=1.0) :
         _n(init(n,q)),
         _q(q),
         _dist(_pdf.begin(), _pdf.end())
      {}
      void reset() {}

      IntType operator()(std::mt19937_64& rng)
      {
         return _dist(rng);
      }

      /// Returns the parameter the distribution was constructed with.
      RealType s() const { return _q; }
      /// Returns the minimum value potentially generated by the distribution.
      result_type min() const { return 1; }
      /// Returns the maximum value potentially generated by the distribution.
      result_type max() const { return _n; }

   private:
      std::vector<RealType>               _pdf;  ///< Prob. distribution
      IntType                             _n;    ///< Number of elements
      RealType                            _q;    ///< Exponent
      std::discrete_distribution<IntType> _dist; ///< Draw generator

      /** Initialize the probability mass function */
      IntType init(const IntType n, const RealType q)
      {
         _pdf.reserve(n+1);
         _pdf.emplace_back(0.0);
         for (IntType i=1; i<=n; i++)
            _pdf.emplace_back(std::pow((double) i, -q));
         return n;
      }
};

/**
 * @param db_path the db path
 * @param sst_dir local sst directory
 * @param remote_sst_dir not "" for non-tail nodes
 * @param target_addr the downstream node's address in the chain, for the non-tail nodes, it's forwarding ops to this address, 
 *                    for the tail node, this target_addr is replicator's address to which it's sending the true reply.
 * @param is_rubble when enabled, compaction is disabled when is_primary sets to false,
 *                   when disabled, all nodes are running flush/compaction
 * @param is_primary set to true for first/primary node in the chain 
 * @param is_tail    set to true for tail node in the chain
 */
rocksdb::DB* GetDBInstance(const string& db_path, const string& sst_dir, 
                                const string& remote_sst_dir,
                                const string& target_addr, 
                                bool is_rubble, bool is_primary, bool is_tail){

   rocksdb::DB* db;
   rocksdb::DBOptions db_options;
   rocksdb::ConfigOptions config_options;
   std::vector<rocksdb::ColumnFamilyDescriptor> loaded_cf_descs;
   rocksdb::Status s = LoadOptionsFromFile(
      /*config_options=*/config_options,
      /*options_file_name=*/"/mnt/sdb/my_rocksdb/rubble/rubble_16gb_config.ini",
      /*db_options=*/&db_options,
      /*cf_descs=*/&loaded_cf_descs
   );
   if (!s.ok()) std::cout << s.getState() << '\n';
   assert(s.ok());

   std::cout << "number of cf: " << loaded_cf_descs.size() << '\n';
   assert(loaded_cf_descs.size() == 1); // We currently only support one ColumnFamily
   
   // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
   db_options.IncreaseParallelism();
   // db_options.is_rubble=is_rubble;
   db_options.is_primary=is_primary;
   db_options.is_tail=is_tail;

   std::cout << "is_rubble: " << db_options.is_rubble << '\n';
   std::cout << "is_primary: " << db_options.is_primary << '\n';
   std::cout << "is_tail: " << db_options.is_tail << '\n';
   
   db_options.target_address=target_addr; //TODO(add target_addr, remote_sst_dir and preallocated_sst_pool_size to option file)

   // for non-tail nodes in rubble mode, it's shipping sst file to the remote_sst_dir;
   if(db_options.is_rubble && !is_tail){
         db_options.remote_sst_dir=remote_sst_dir;
         std::cout << "remote sst dir: " << db_options.remote_sst_dir << std::endl;
   }
   
   uint64_t target_size = 10000000000;
   db_options.db_paths.emplace_back(rocksdb::DbPath(sst_dir, 10000000000));
   
   rocksdb::ColumnFamilyOptions cf_options = loaded_cf_descs[0].options;

   if(db_options.target_address != ""){
      db_options.channel = grpc::CreateChannel(db_options.target_address, grpc::InsecureChannelCredentials());
   }

   if(db_options.is_rubble){
      std::string rubble_info_log_fname;
      std::string rubble_log_path {"/mnt/sdb/my_rocksdb/rubble/log"};
      std::shared_ptr<rocksdb::Logger> logger = nullptr;
      db_options.env = rocksdb::Env::Default();
      db_options.env->CreateDirIfMissing(rubble_log_path).PermitUncheckedError(); 
      if(db_options.is_primary){
         rubble_info_log_fname = rubble_log_path.append("/primary");
      }else if(db_options.is_tail){
         rubble_info_log_fname = rubble_log_path.append("/tail");
      }else{
         rubble_info_log_fname = rubble_log_path.append("/secondary");
      }

      rocksdb::Status s = rocksdb::NewEnvLogger(rubble_info_log_fname, db_options.env, &logger);
      if (logger.get() != nullptr) {
         logger->SetInfoLogLevel(db_options.info_log_level);
      }
      if(!s.ok()){
         std::cout << "Error creating log : " << s.ToString() << std::endl;
         assert(false);
         logger = nullptr;
      }

      db_options.rubble_info_log = logger;
      //ignore this flag for now, always set to true.
      db_options.disallow_flush_on_secondary = true;

      db_options.piggyback_version_edits = true;
      db_options.edits = std::make_shared<Edits>();

      if(!db_options.is_tail && !db_options.piggyback_version_edits){
         assert(db_options.target_address != "");
         db_options.sync_client = std::make_shared<SyncClient>(db_options.channel);
      }
      // the sst pool size should be set to size_of_your_data/sst_file_size
      // set to 450 for 5GB db and 16MB sst_file_size
      db_options.preallocated_sst_pool_size = 450;
      // db_options.preallocated_sst_pool_size = db_options.db_paths.front().target_size / (((cf_options.write_buffer_size >> 20) + 1) << 20);
      db_options.sst_bit_map = std::make_shared<SstBitMap>(db_options.preallocated_sst_pool_size, db_options.rubble_info_log);
   }

   if(!db_options.is_primary){
      db_options.use_direct_reads = true;
   }else{
      db_options.use_direct_reads = false;
   }

   std::cout << "write_buffer_size: " << cf_options.write_buffer_size << '\n';
   std::cout << "target_file_size_base: " << cf_options.target_file_size_base << '\n';
   rocksdb::Options options(db_options, cf_options);

   if(db_options.is_rubble && !db_options.is_primary){
      // make sure compaction is disabled on the secondary nodes in rubble mode
      options.compaction_style = rocksdb::kCompactionStyleNone;
   }

   // open DB
   s = rocksdb::DB::Open(options, db_path, &db);
   if(!s.ok()){
      std::cout << "DB open failed : " << s.ToString() << std::endl;
   }
   assert(s.ok());
   return db;
}


void RunServer(rocksdb::DB* db, const std::string& server_addr) {
  
   RubbleKvServiceImpl service(db);
   grpc::EnableDefaultHealthCheckService(true);
   grpc::reflection::InitProtoReflectionServerBuilderPlugin();
   ServerBuilder builder;

   builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
   std::cout << "Server listening on " << server_addr << std::endl;
   builder.RegisterService(&service);
   std::unique_ptr<Server> server(builder.BuildAndStart());
   server->Wait();
}