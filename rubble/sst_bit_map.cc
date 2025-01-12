#include "sst_bit_map.h"
#include <iostream>
#include <assert.h>
#include <logging/logging.h>

SstBitMap::SstBitMap(int pool_size, int max_num_mems_in_flush,
        bool is_primary, int rf,
        std::shared_ptr<rocksdb::Logger> logger, 
        std::shared_ptr<rocksdb::Logger> map_logger)
    :size_(pool_size), num_big_slots_(100),
    max_num_mems_in_flush_(max_num_mems_in_flush),
    logger_(logger),
    map_logger_(map_logger){
        int num_big_slots = (max_num_mems_in_flush_ - 1) *num_big_slots_;
        slots_.reserve(size_ + 1 + num_big_slots);
        slots_.assign(size_ + 1 + num_big_slots, 0);
        slot_usage_.reserve(size_ + 1 + num_big_slots);
        slot_usage_.assign(size_ + 1 + num_big_slots, 0);
        next_available_slot_.push_back(1); // default slot start from 1
        for(int i = 0; i < max_num_mems_in_flush_ - 1; i++){
            // offset start of big sst slots
            next_available_slot_.push_back(size_ + 1 + i * num_big_slots_);
        }
        for(int i = 0; i < max_num_mems_in_flush_; i++){
            num_slots_taken_.push_back(0);
        }

        if (is_primary) {
            // primary needs to know which secondary node has freed the slot
            slot_initial_usage_ = (1 << rf) - 2;
        } else {
            slot_initial_usage_ = 1;
        }

        // if (is_tail || is_primary) {
        //     slot_initial_usage_ = 1;
        // } else {
        //     slot_initial_usage_ = 1;
        // }
    }


int SstBitMap::TakeOneAvailableSlot(uint64_t file_num, int times){
    // by default RocksDB sets max_write_buffer_number to 2,
    // so flushes only 1 memtable each time
    assert(times > 0);

    std::lock_guard<std::mutex> lk{mu_};

    if (num_slots_taken_[0] == size_ 
        || (times >= 2 && num_slots_taken_[times - 1] == num_big_slots_)) {
        std::cerr << "run out of slots, try later\n";
        return -1;
    }
    
    int start, end;
    if (times == 1) {
        start = 1;
        end = size_;
    } else {
        start = size_ + 1 + (times - 2)*num_big_slots_;
        end = start + num_big_slots_ - 1;
    }
   
    int slot_num = next_available_slot_[times - 1];
    // we need to do elaborate checking because some slots might have been freed
    // we would like to maximize the utilization
    if (slots_[slot_num] != 0) { // if this slot has been taken
        int cur = slot_num + 1;
        while (cur <= end && slots_[cur] != 0) {
            cur++;
        }
        // circular buffer, loop from start again to find available slots
        if(cur > end){
            cur = start;
            while(cur <= end && slots_[cur] != 0){
                cur++;
            }
            // if still couldn't find it
            if(cur > end){
                std::cerr << "times: " << times << ", total " << num_slots_taken_[times - 1] << " taken\n";
                return -1;
            }
        }
        slot_num = cur;
    }
    
    slots_[slot_num] = file_num;
    slot_usage_[slot_num] = slot_initial_usage_;
    RUBBLE_LOG_INFO(map_logger_, "%lu %d\n", file_num, times);
    RUBBLE_LOG_INFO(logger_, "Take Slot (%lu , %d)\n", file_num, slot_num);
    // std::cout << "[sst bitmap] " << "File " << file_num << " takes slot " << slot_num << std::endl;
    LogFlush(map_logger_);
    LogFlush(logger_);
    
    file_slots_.emplace(file_num, slot_num);
    num_slots_taken_[times - 1]++;

    // CheckNumSlotsTaken();
    if(slot_num == end){
        next_available_slot_[times - 1]= start;
    }else{
        next_available_slot_[times - 1] = slot_num + 1;
    }

    return slot_num;
}

bool SstBitMap::TakeSlotsInBatch(const std::vector<std::pair<uint64_t, int>>& files_info) {
    std::lock_guard<std::mutex> lk{mu_};
    int slots_to_take = files_info.size();

    if (num_slots_taken_[0] + slots_to_take > size_) {
        std::cerr << "run out of slots, don't have " << slots_to_take << " slots available\n";
        return false;
    }
    
    int start = 1, end = size_;
    std::map<uint64_t, int> assigned_file_slots;
    
    for (int i = 0; i < slots_to_take; i++) {
        uint64_t file_num = files_info[i].first;
        int times = files_info[i].second;
        // we temporarily set each sst to be 17m
        assert(times == 1);

        int slot_num = next_available_slot_[times - 1];
        if (slots_[slot_num] != 0) {
            int cur = slot_num + 1;
            while (cur <= end && slots_[cur] != 0) {
                cur++;
            }

            if (cur > end) {
                cur = start;
                while (cur <= end && slots_[cur] != 0){
                    cur++;
                }
                if (cur > end) {
                    std::cerr << "run out of slots, don't have " << slots_to_take << " slots available\n";
                    return false;
                }
            }
            slot_num = cur;
        }
        if (slot_num == end) {
            next_available_slot_[0] = start;
        } else {
            next_available_slot_[0] = slot_num + 1;
        }
        assigned_file_slots[file_num] = slot_num;
    }

    for (const auto& p : assigned_file_slots) {
        uint64_t file_num = p.first;
        int slot_num = p.second;

        slots_[slot_num] = file_num;
        slot_usage_[slot_num] = slot_initial_usage_;
        file_slots_.emplace(file_num, slot_num);
        num_slots_taken_[0]++;  // as we suppose times=1 for now

        assert(num_slots_taken_[0] <= size_);
        
        // std::cout << "[sst bitmap] TakeSlotInBatch: " << "File " << file_num << " takes slot " << slot_num <<
        //     ", remains " << size_ - num_slots_taken_[0] << " free slots" << std::endl;
    }

    return true;
}

int SstBitMap::GetAvailableSlots(int times) {
    return size_ - num_slots_taken_[times - 1];
}

// Since the tail is dead, we erase its bit in all slots
void SstBitMap::RemoveTail(int rf) {
    slot_initial_usage_ = (1 << (rf - 1)) - 2;
    int tail_rid = rf -  1;
    int tail_bit = 1 << tail_rid;
    std::set<uint64_t> tail_used_files;
    
    for (size_t i = 0; i < slot_usage_.size(); i++) {
        if (slot_usage_[i] & tail_bit) {
            tail_used_files.insert(i);
        }
    }

    FreeSlot(tail_used_files, tail_rid, true);
}

void SstBitMap::WaitForFreeSlots(const std::map<int, int>& needed_slots) {
    std::unique_lock<std::mutex> lock{mu_};
    bitmap_full_cond_.wait(lock, [&] { 
        for (const auto& p : needed_slots) {
            int times = p.first;
            int slots = p.second;
            if (GetAvailableSlots(times) < slots)
                return false;
        }
        return true;
    });
    lock.unlock();
}

void SstBitMap::NotifyFreeSlot() {
    bitmap_full_cond_.notify_all();
}

void SstBitMap::CheckNumSlotsTaken(){
    int total_slots_taken = 0;
    for(int count : num_slots_taken_){
        total_slots_taken += count;
    }
    int total_file_slots = static_cast<int>(file_slots_.size()); //initialization
    std::vector<int> slot_taken(num_slots_taken_.capacity(), 0);
    
    // TODO: for debugging purposes only I think...
    if(total_file_slots != total_slots_taken){
        std::unordered_map<uint64_t, int>::iterator it;
        for(it = file_slots_.begin(); it != file_slots_.end(); ++it){
            if(it->second <= size_){ // if normal, idx = 0
                slot_taken[0]++;
            }else{ // else calculate times
                int idx = (it->second - size_) /num_big_slots_ + 1;
                slot_taken[idx]++;
            }
        }
        for(int i = 0 ; i < static_cast<int>(slot_taken.size()); i++){
            std::cout << " ( " << i << " : " << slot_taken[i] << " , " << num_slots_taken_[i] << " ) \n";
        }
    }
    assert(static_cast<int>(file_slots_.size()) == total_slots_taken);
}

bool SstBitMap::CheckSlotFreed(int slot_num) {
    return slot_usage_[slot_num] == 0;
}

void SstBitMap::FreeSlot(std::set<uint64_t> file_nums, int rid, bool notify) {
    std::lock_guard<std::mutex> lk{mu_};

    for (uint64_t file_num : file_nums) {
        int slot_num = file_slots_[file_num];
        // assert(slot_usage_[slot_num] == 1 || slot_usage_[slot_num] == 2);
        slot_usage_[slot_num] &= ~(1 << rid);

        // std::cout << "[sst bitmap] " << "try to free slot " << slot_num << " of file " << file_num << std::endl;

        if (slot_usage_[slot_num] == 0) {
            RUBBLE_LOG_INFO(map_logger_, "%lu\n", file_num);
            RUBBLE_LOG_INFO(logger_, "Free Slot (%d , %lu) \n", slot_num, slots_[slot_num]);
            // printf("Free Slot (%d , %lu) \n", slot_num, slots_[slot_num]);
            LogFlush(map_logger_);
            LogFlush(logger_);
            int idx = slot_num <= size_ ? 0 : ((slot_num - size_ - 1) /num_big_slots_  + 1 );
            num_slots_taken_[idx]--;
            slots_[slot_num] = 0;
            file_slots_.erase(file_num);

            // std::cout << "[sst bitmap] " << "successfully free slot " << slot_num << " of file " << file_num << std::endl;   
        }

        // std::cout << "[sst bitmap] remains " << size_ - num_slots_taken_[0] << " free slots" << std::endl;
    }

    if (notify) {
        NotifyFreeSlot();
    }
}
    
uint64_t SstBitMap::GetSlotFileNum(int slot_num){
    std::lock_guard<std::mutex> lk{mu_};
    // why this assert here? what about big slots
    assert(slot_num <= size_);
    return slots_[slot_num];
}

int SstBitMap::GetFileSlotNum(uint64_t file_num){
    std::lock_guard<std::mutex> lk{mu_};
    if (file_slots_.count(file_num) == 0)
        return -1;
    // assert(file_slots_.find(file_num) != file_slots_.end());
    return file_slots_[file_num];
}

void SstBitMap::TakeSlot(uint64_t file_num, int slot_num, int times) {
    assert(times > 0);
    std::lock_guard<std::mutex> lk{mu_};

    RUBBLE_LOG_INFO(map_logger_, "%lu %d\n", file_num, times);
    RUBBLE_LOG_INFO(logger_, "Take Slot (%lu , %d)\n", file_num, slot_num);
    // printf("Take Slot (%lu , %d)\n", file_num, slot_num);
    
    LogFlush(map_logger_);
    LogFlush(logger_);

    slots_[slot_num] = file_num;
    slot_usage_[slot_num] = slot_initial_usage_;
    file_slots_[file_num] = slot_num;
    num_slots_taken_[times-1]++;

    // std::cout << "[sst bitmap] TakeSlot:" << "File " << file_num << " takes slot " << slot_num <<
    //     " remains " << size_ - num_slots_taken_[0] << " free slots" << std::endl;

    int start, end;
    if(times == 1){
        start = 1;
        end = size_;
    }else{
        start = size_ + 1 + (times - 2)*num_big_slots_;
        end = start + num_big_slots_ - 1;
    }
    if(slot_num == end){
        next_available_slot_[times - 1]= start;
    }else{
        next_available_slot_[times - 1] = slot_num + 1;
    }
}
