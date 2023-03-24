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
    std::vector<FileInfo> files_;
    std::vector<std::string> edits_json_;
    const ImmutableDBOptions* db_options_;
    std::vector<ShipThreadArg*> dependants_;

    ShipThreadArg(const ImmutableDBOptions* db_options) :
        files_(),
        edits_json_(),
        db_options_(db_options),
        dependants_() {};
    
    int GetEditId() const {
        assert(edits_json_.size() > 0 && edits_json_[0].size() > 0);
        nlohmann::json j = nlohmann::json::parse(edits_json_[0]);
        return j["Id"].get<int>();

    }
};

bool HasEditJson(ShipThreadArg* const a);

void AddEditJson(ShipThreadArg* const a, std::string json);

void AddDependant(ShipThreadArg* const a, ShipThreadArg* const b);

void PrepareFile(ShipThreadArg* const sta, AlignedBuffer& buf);

void AddFile(ShipThreadArg* const sta, uint64_t times, uint64_t file_number);

bool NeedShipSST(const ImmutableDBOptions* db_options);

void ApplyDownstreamSstSlotDeletion(ShipThreadArg* sta, const nlohmann::json& reply_json);

void ShipSST(FileInfo& file, const std::vector<std::string>& remote_sst_dirs, ShipThreadArg *sta);

// void ShipJobTakeSlot(ShipThreadArg* sta, FileInfo& f);

std::string VersionEditsToJson(uint64_t next_file_number,
                               uint64_t log_and_apply_counter,
                               const autovector<VersionEdit*>& edit_lists);

SyncClient* GetSyncClient(const ImmutableDBOptions* db_options_);

bool AddedFiles(const autovector<autovector<VersionEdit*>>& edit_lists);

void BGWorkShip(void* arg);

void UnscheduleShipCallback(void* arg);
}