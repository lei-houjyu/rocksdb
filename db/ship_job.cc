#include "db/ship_job.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>
#include <ctime>
#include <iomanip>

namespace ROCKSDB_NAMESPACE {
uint64_t CheckSum(const char* src, const size_t len) {
    uint64_t sum = 0;
    for (size_t i = 0; i < len; i++) {
        sum += (uint64_t)src[i];
    }
    return sum;
}

bool HasEditJson(ShipThreadArg* const a) {
    for (std::string json : a->edits_json_) {
        if (json.length() > 0) {
            return true;
        }
    }
    return false;
}

void AddEditJson(ShipThreadArg* const a, std::string json) {
    a->edits_json_.emplace_back(json);
}

void AddDependant(ShipThreadArg* const a, ShipThreadArg* const b) {
    a->dependants_.emplace_back(b);
    // printf("Add %p into %p's dependants\n", b, a);
}

void PrepareFile(ShipThreadArg* const sta, AlignedBuffer& buf) {
    FileInfo& file = sta->files_.back();
    file.len_ = buf.Capacity();
    file.buf_ = buf.BufferStart();
    file.beg_ = buf.Release();
    // file.checksum_ = CheckSum(file.buf_, file.len_);
}

void AddFile(ShipThreadArg* const sta, uint64_t times, uint64_t file_number) {
    sta->files_.emplace_back();
    FileInfo& file = sta->files_.back();
    file.times_ = times;
    file.file_number_ = file_number;
    // TODO:Sheng
    // file.slot_number_ = sta->db_options_->sst_bit_map->TakeOneAvailableSlot(file_number, times);
}

bool NeedShipSST(const ImmutableDBOptions* db_options) {
    // Only primary nodes in Rubble who have secondary nodes
    return db_options->is_rubble && db_options->is_primary && !db_options->is_tail;
}

void ReadSST(char *file_name, unsigned char *buf, int size) {
    int fd;
    do {
        fd = open(file_name, O_RDONLY | O_DIRECT | O_DSYNC, 0755);
    } while (fd < 0 && errno == EINTR);

    ssize_t done = read(fd, buf, size);
    if (done != size) {
        std::cout << "[ReadSST] While reading file " << file_name << " errno " << std::strerror(errno) << std::endl;
        assert(false);
    }

    close(fd);
}

void WriteSST(char *file_name, unsigned char *buf, int size) {
    int fd;
    do {
        fd = open(file_name, O_WRONLY | O_DIRECT | O_DSYNC, 0755);
    } while (fd < 0 && errno == EINTR);

    ssize_t done = write(fd, buf, size);
    if (done != size) {
        std::cout << "[WriteSST] While writing file " << file_name << " errno " << std::strerror(errno) << std::endl;
        assert(false);
    }
    
    close(fd);
}

void RecoverSST(std::vector<char *>* src_name, std::vector<char *>* dst_name, int size) {
    unsigned char * buf = nullptr;
    int ret = posix_memalign((void **)&buf, 512, size);
    if (ret) {
        perror("[ShipSST] posix_memalign failed");
        assert(false);
    }
    
    for (size_t i = 0; i < src_name->size(); i++) {
        ReadSST(src_name->at(i), buf, size);
        WriteSST(dst_name->at(i), buf, size);
        printf("[RecoverSST] %s %s\n", src_name->at(i), dst_name->at(i));
    }

    free(buf);
}

void ShipSST(FileInfo& file, const std::vector<std::string>& remote_sst_dirs, ShipThreadArg* sta) {
    // std::cout << "[ShipSST] file_number: " << file.file_number_ << " slot_number: " << file.slot_number_ << std::endl;

    for (std::string dir : remote_sst_dirs) {
        std::string fname = dir + "/" + std::to_string(file.slot_number_);
        int r_fd;
        do {
            r_fd = open(fname.c_str(), O_WRONLY | O_DIRECT | O_DSYNC, 0755);
        } while (r_fd < 0 && errno == EINTR);

        if (r_fd < 0) {
            std::cout << "While open a file for appending " << fname << " errno " << std::strerror(errno) << std::endl;
            assert(false);
        }

        ssize_t done = write(r_fd, file.buf_, file.len_);
        if (done != (ssize_t)file.len_) {
            std::cout << "while appending to file " << fname << " errno " << std::strerror(errno) << std::endl;
            assert(false);
        }

        sta->db_options_->shipped_files_nvmeof->fetch_add(1);
        ROCKS_LOG_INFO(sta->db_options_->info_log, 
            "Shipped SST file %s via NVMe-oF, total count: %d", fname.c_str(),
            sta->db_options_->shipped_files_nvmeof->load());

        // assert(fsync(r_fd) == 0);

        // int w_fd;
        // fname = "/mnt/data/dump/" + dir.substr(16) + "/" + std::to_string(file.slot_number_) + ".sst";
        // do {
        //     w_fd = open(fname.c_str(), O_WRONLY | O_DIRECT | O_DSYNC | O_CREAT, 0755);
        // } while (w_fd < 0 && errno == EINTR);
        // if (w_fd < 0) {
        //     std::cout << "While open a file for appending " << fname << " errno " << std::strerror(errno) << std::endl;
        //     assert(false);
        // }

        // done = write(w_fd, file.buf_, file.len_);
        // if (done != (ssize_t)file.len_) {
        //     std::cout << "while appending to file " << fname << " errno " << std::strerror(errno) << std::endl;
        //     assert(false);
        // }
        // close(w_fd);

        // uint64_t got = CheckSum(file.buf_, file.len_);
        // uint64_t expected = file.checksum_;
        // if (got != expected) {
        //     std::cout << "[ShipSST] checksum mismatch expected: " << expected
        //               << " got: " << got << " sta: " << reinterpret_cast<std::size_t>(sta) 
        //               << std::endl;
        //     assert(false);
        // }

        close(r_fd);
    }

    delete [] file.beg_;
}

std::string VersionEditsToJson(uint64_t next_file_number,
                                  uint64_t log_and_apply_counter,
                                  const autovector<VersionEdit*>& edit_list) {
    nlohmann::json res;

    if (edit_list.back()->IsFlush()) {
        res["IsFlush"] = true;
        res["BatchCount"] = edit_list.back()->GetBatchCount();
    } else if (edit_list.back()->IsTrivialMove()) {
        res["IsTrivial"] = true;
    }

    res["NextFileNum"] = next_file_number;
    std::vector<std::string> version_edits;

    for (auto e: edit_list) {
        version_edits.emplace_back(e->DebugJSON((int) log_and_apply_counter, false));
    }

    nlohmann::json j_vec(version_edits);
    res["EditList"] = j_vec;
    res["Id"] = log_and_apply_counter;

    return res.dump();
}

SyncClient* GetSyncClient(const ImmutableDBOptions* db_options_) {
    thread_local SyncClient *client = nullptr;

    std::shared_ptr<grpc::Channel> channel;
    channel = db_options_->channel;

    if (client == nullptr) {
        do {
            if (client != nullptr) {
                delete client;
            }
            client = new SyncClient(channel);
        } while (channel->GetState(false) != 2);
        // std::cout << "thread " << std::this_thread::get_id()
        //             << " creates sync client " << client
        //             << " state " << channel->GetState(false) << std::endl;
    }

    return client;
}

SyncClient* GetPrimarySyncClient(const ImmutableDBOptions* db_options_) {
    thread_local SyncClient *primary_client = nullptr;

    std::shared_ptr<grpc::Channel> channel;
    channel = db_options_->primary_channel;

    if (primary_client == nullptr) {
        do {
            if (primary_client != nullptr) {
                delete primary_client;
            }
            primary_client = new SyncClient(channel);
        } while (channel->GetState(false) != 2);
        // std::cout << "thread " << std::this_thread::get_id()
        //             << " creates sync client " << primary_client
        //             << " state " << channel->GetState(false) << std::endl;
    }
    

    return primary_client;
}

// Test if the edit_lists contains the addedFiles fields. If not, it's
// generated by a recovery and doesn't need to be sent out
bool AddedFiles(const autovector<autovector<VersionEdit*>>& edit_lists) {
  bool res = true;
  for (auto list : edit_lists) {
    for (auto edit : list) {
      if (edit->GetNewFiles().empty()) {
        res = false;
      } else {
        // we can't handle partially empty edit_lists
        assert(res);
      }
    }
  }
  return res;
}

// void ApplyDownstreamSstSlotDeletion(ShipThreadArg* sta, const nlohmann::json& reply_json) {
//     std::stringstream ss;
//     bool did_deletion = false;
//     for (const auto& deleted_slot : reply_json["DeletedSlots"]) {
//         int slot = deleted_slot.get<int>();
//         // uint64_t filenumber = sta->db_options_->sst_bit_map->GetSlotFileNum(slot);
//         sta->db_options_->sst_bit_map->FreeSlot2(slot);
//         ss << slot << ',';
//         did_deletion = true;
//     }
//     if (did_deletion)
//         std::cout << "[sync] apply deletion from downstream nodes, deleted slot: " << ss.str() << std::endl;
// }

// void ShipJobTakeSlot(ShipThreadArg* sta, FileInfo& f) {
//     while (true) {
//         f.slot_number_ = sta->db_options_->sst_bit_map->TakeOneAvailableSlot(f.file_number_, f.times_);
//         if (f.slot_number_ == -1) {
//             std::cout << "ShipJobTakeSlot " << sta << " sst bitmap is full, waiting for a free slot..." << std::endl;
//             sta->db_options_->sst_bit_map->WaitForFreeSlots();
//             continue;
//         }
//         return;
//     }
// }

void BGWorkShip(void* arg) {
    // TODO: print edit ID timestamp
    // auto t1_now = std::chrono::system_clock::now();
    // auto t1 = std::chrono::system_clock::to_time_t(t1_now);
    // auto tm_1 = *std::localtime(&t1);
    ShipThreadArg* sta = reinterpret_cast<ShipThreadArg*>(arg);

    // 1. ship SST file to secondary nodes
    // 1.1 find available slots in the SST pool
    // int sst_slot = sta->db_options_->sst_bit_map->TakeOneAvailableSlot(sta->sst_number_, sta->times_);

    // 1.2 ship SST file via NVMe-oF
    // std::stringstream ss;
    std::vector<std::pair<uint64_t, int>> files_info;
    std::map<int, int> needed_slots;

    for (FileInfo f : sta->files_) {
        files_info.push_back({f.file_number_, f.times_});
        needed_slots[f.times_]++;
        // ss << f.file_number_ << ",";
    }
    for (ShipThreadArg* const s : sta->dependants_) {
        for (FileInfo f : s->files_) {
            files_info.push_back({f.file_number_, f.times_});
            needed_slots[f.times_]++;
            // ss << f.file_number_ << ",";
        }
    }

    // std::cout << "about to take slots for these files in batch: " << ss.str() << std::endl;
    // try to take slots for all sst files
    // TODO: print edit ID timestamp
    while (!sta->db_options_->sst_bit_map->TakeSlotsInBatch(files_info)) {
        std::cout << "not able to take slots in batch, wait for freeing..." << std::endl;
        sta->db_options_->sst_bit_map->WaitForFreeSlots(needed_slots);
        std::cout << "Wake up! now I have " << sta->db_options_->sst_bit_map->GetAvailableSlots(1) << " free slots!" << std::endl;
    }
    // std::cout << "take slots for files: " << ss.str() << "successfully" << std::endl;
    // ss.str(std::string());

    // auto t2_now = std::chrono::system_clock::now();
    // auto t2 = std::chrono::system_clock::to_time_t(t2_now);
    // auto tm_2 = *std::localtime(&t2);

    // then ship sst
    // TODO: print edit ID timestamp
    for (FileInfo f : sta->files_) {
        int slot = sta->db_options_->sst_bit_map->GetFileSlotNum(f.file_number_);
        f.slot_number_ = slot;
        ShipSST(f, sta->db_options_->remote_sst_dirs, sta);
        // ss << "file " << f.file_number_ << " takes slot " << slot << ", ";
    }
    for (ShipThreadArg* const s : sta->dependants_) {
        for (FileInfo f : s->files_) {
            int slot = sta->db_options_->sst_bit_map->GetFileSlotNum(f.file_number_);
            f.slot_number_ = slot;
            ShipSST(f, sta->db_options_->remote_sst_dirs, sta);
            // ss << "file " << f.file_number_ << " takes slot " << slot << ", ";
        }
    }

    // auto t3_now = std::chrono::system_clock::now();
    // auto t3 = std::chrono::system_clock::to_time_t(t3_now);
    // auto tm_3 = *std::localtime(&t3);

    // auto now = std::chrono::system_clock::now();
    // auto now_c = std::chrono::system_clock::to_time_t(now);
    // std::tm tm = *std::localtime(&now_c);
    // int ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;

    // std::cout << std::put_time(&tm, "%y:%m:%d %H:%M:%S.") << std::setfill('0') << std::setw(3) << ms
    // << " shipped sst: " << ss.str() << std::endl;
    
    
    // 2. send version edits to secondary nodes
    // printf("[BGWorkShip] sta_ %p json_size %ld\n", sta, sta->edits_json_.size());
    for (std::string json : sta->edits_json_) {
        // printf("[BGWorkShip] sta_ %p json_length %ld\n", sta, json.length());
        if (json.length() > 0) {
            // fill in the slot info in the json here
            // printf("[BGWorkShip] sta: %p edits_json: %s\n", sta, json.data());
            nlohmann::json j = nlohmann::json::parse(json), j_new;

            for (auto& it : j.items()) {
                if (it.key() != "EditList") {
                    j_new[it.key()] = it.value();
                } else {
                    nlohmann::json edit_array = nlohmann::json::array();
                    for (auto& edit_string : it.value().get<std::vector<std::string>>()) {
                        nlohmann::json edit_json = nlohmann::json::parse(edit_string), edit_json_new;
                        for (auto& edit_json_it : edit_json.items()) {
                            if (edit_json_it.key() != "AddedFiles") {
                                edit_json_new[edit_json_it.key()] = edit_json_it.value();
                            } else {
                                nlohmann::json added_files_array = nlohmann::json::array();
                                
                                for (auto& added_files_it : edit_json_it.value().items()) {
                                    uint64_t filenumber;
                                    nlohmann::json added_file_json = added_files_it.value();
                                    added_file_json["Slot"] = sta->db_options_->sst_bit_map->GetFileSlotNum(added_file_json["FileNumber"].get<uint64_t>());
                                    added_files_array.push_back(added_file_json);
                                }
                                edit_json_new["AddedFiles"] = added_files_array;
                            }
                        }
                        edit_array.push_back(edit_json_new.dump());
                    }
                    
                    j_new["EditList"] = edit_array;
                }
            }
            // std::cout << "modified json: " << j_new.dump() << std::endl;
            
            // auto t4_now = std::chrono::system_clock::now();
            // auto t4 = std::chrono::system_clock::to_time_t(t4_now);
            // auto tm_4 = *std::localtime(&t4);
            SyncClient* client = GetSyncClient(sta->db_options_);
            client->Sync(j_new.dump(), sta->db_options_->rid);

            // std::cout << "[ship event] edit id: " << j_new["Id"].get<uint64_t>() 
            //     << ", t1: "
            //     << std::put_time(&tm_1, "%H:%M:%S.") << std::setfill('0') << std::setw(3)
            //     << std::chrono::duration_cast<std::chrono::milliseconds>(t1_now.time_since_epoch()).count() % 1000
            //     << ", t2: "
            //     << std::put_time(&tm_2, "%H:%M:%S.") << std::setfill('0') << std::setw(3)
            //     << std::chrono::duration_cast<std::chrono::milliseconds>(t2_now.time_since_epoch()).count() % 1000
            //     << ", t3: "
            //     << std::put_time(&tm_3, "%H:%M:%S.") << std::setfill('0') << std::setw(3)
            //     << std::chrono::duration_cast<std::chrono::milliseconds>(t3_now.time_since_epoch()).count() % 1000
            //     << ", t4: "
            //     << std::put_time(&tm_4, "%H:%M:%S.") << std::setfill('0') << std::setw(3)
            //     << std::chrono::duration_cast<std::chrono::milliseconds>(t4_now.time_since_epoch()).count() % 1000 << std::endl;
        }
    }

    for (ShipThreadArg* const s : sta->dependants_) {
        delete s;
    }
    delete reinterpret_cast<ShipThreadArg*>(arg);
}

void UnscheduleShipCallback(void* arg) {
    ShipThreadArg* sta = reinterpret_cast<ShipThreadArg*>(arg);
    
    for (ShipThreadArg* const s : sta->dependants_) {
        delete s;
    }

    delete reinterpret_cast<ShipThreadArg*>(arg);
}
}