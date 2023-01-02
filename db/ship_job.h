#pragma once

#include "util/aligned_buffer.h"
#include "options/db_options.h"
#include "db/version_edit.h"
#include "util/autovector.h"
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include "rubble/sync_client.h"
#include "util/aligned_buffer.h"
#include <vector>

namespace ROCKSDB_NAMESPACE {
struct FileInfo {
    char * beg_;
    char * buf_;
    size_t len_;
    uint64_t times_;
    int slot_number_;
    uint64_t file_number_;
    uint64_t checksum_;

    FileInfo() : 
        beg_(nullptr),
        buf_(nullptr),
        len_(0),
        times_(1),
        slot_number_(0),
        file_number_(0),
        checksum_(0) {};
};

struct ShipThreadArg {
    std::string edits_json_;
    std::vector<FileInfo> files_;
    const ImmutableDBOptions* db_options_;

    ShipThreadArg(const ImmutableDBOptions* db_options) :
        edits_json_(),
        files_(),
        db_options_(db_options) {};
};

void PrepareFile(ShipThreadArg* sta, AlignedBuffer& buf);

void AddFile(ShipThreadArg* sta, uint64_t times, uint64_t file_number);

bool NeedShipSST(const ImmutableDBOptions* db_options);

void ShipSST(FileInfo& file, const std::vector<std::string>& remote_sst_dirs, ShipThreadArg *sta);

std::string VersionEditsToJson(uint64_t next_file_number,
                               uint64_t log_and_apply_counter,
                               const autovector<VersionEdit*>& edit_lists);

SyncClient* GetSyncClient(const ImmutableDBOptions* db_options_);

bool AddedFiles(const autovector<autovector<VersionEdit*>>& edit_lists);

void BGWorkShip(void* arg);

void UnscheduleShipCallback(void* arg);
}