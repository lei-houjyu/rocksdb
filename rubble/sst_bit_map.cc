#include "sst_bit_map.h"
#include <iostream>
#include <assert.h>
#include <logging/logging.h>

SstBitMap::SstBitMap(int pool_size, int max_num_mems_in_flush,
        std::shared_ptr<rocksdb::Logger> logger, 
        std::shared_ptr<rocksdb::Logger> map_logger)
    :size_(pool_size), num_big_slots_(size_ / 50),
    max_num_mems_in_flush_(max_num_mems_in_flush),
    logger_(logger),
    map_logger_(map_logger){
        int num_big_slots = (max_num_mems_in_flush_ - 1) *num_big_slots_;
        slots_.reserve(size_ + 1 + num_big_slots);
        slots_.assign(size_ + 1 + num_big_slots, 0);
        next_available_slot_.push_back(1); // default slot start from 1
        for(int i = 0; i < max_num_mems_in_flush_ - 1; i++){
            // offset start of big sst slots
            next_available_slot_.push_back(size_ + 1 + i * num_big_slots_);
        }
        for(int i = 0; i < max_num_mems_in_flush_; i++){
            num_slots_taken_.push_back(0);
        }
    }


int SstBitMap::TakeOneAvailableSlot(uint64_t file_num, int times){
    std::unique_lock<std::mutex> lk{mu_};

    if(num_slots_taken_[0] == size_ || (times >= 2 && num_slots_taken_[times - 1] == num_big_slots_)){
        std::cerr << "run out of slots, please choose a larger pool size\n";
        assert(false);
    }
    
    int start, end;
    if(times == 1){
        start = 1;
        end = size_;
    }else{
        start = size_ + 1 + (times - 2)*num_big_slots_;
        end = start + num_big_slots_ - 1;
    }
   
    int slot_num = next_available_slot_[times - 1];
    // we need to do elaborate checking because some slots might have been freed
    // we would like to maximize the utilization
    if(slots_[slot_num] != 0){ // if this slot has been taken
        int cur = slot_num + 1;
        while(cur <= end && slots_[cur] != 0){
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
                std::cerr << "total " << num_slots_taken_[times - 1] << " taken\n";
                assert(false);
            }
        }
        slot_num = cur;
    }
    
    slots_[slot_num] = file_num;
    RUBBLE_LOG_INFO(map_logger_, "%lu %d\n", file_num, times);
    RUBBLE_LOG_INFO(logger_, "Take Slot (%lu , %d)\n", file_num, slot_num);
    file_slots_.emplace(file_num, slot_num);
    // std::cout << "file " << file_num << " took slot " << next_available_slot_ << std::endl;
    num_slots_taken_[times - 1]++;
    // std::cout << "num slots taken : " << num_slots_taken_ << ", file_slots size : " << file_slots_.size() << std::endl;

    CheckNumSlotsTaken();
    if(slot_num == end){
        next_available_slot_[times - 1]= start;
    }else{
        next_available_slot_[times - 1] = slot_num + 1;
    }

    return slot_num;
}

void SstBitMap::CheckNumSlotsTaken(){
    int total_slots_taken = 0;
    for(int count : num_slots_taken_){
        total_slots_taken += count;
    }
    int total_file_slots = static_cast<int>(file_slots_.size()); //initialization
    std::vector<int> slot_taken;
    
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

int SstBitMap::FreeSlot(uint64_t file_num){
    std::unique_lock<std::mutex> lk{mu_};
    // assert that the file num in file_slots map
    assert(file_slots_.find(file_num) != file_slots_.end());
    RUBBLE_LOG_INFO(map_logger_, "%lu\n", file_num);
    int slot_num = file_slots_[file_num];
    int idx = slot_num <= size_ ? 0 : ((slot_num - size_ - 1) /num_big_slots_  + 1 );
    num_slots_taken_[idx]--; 
    file_slots_.erase(file_num);
    return slot_num;
}

void SstBitMap::FreeSlot(std::set<uint64_t> file_nums){
    std::unique_lock<std::mutex> lk{mu_};
    for(uint64_t file_num : file_nums){
        assert(file_slots_.find(file_num) != file_slots_.end());
        int slot_num = file_slots_[file_num];
        RUBBLE_LOG_INFO(map_logger_, "%lu\n", file_num);
        RUBBLE_LOG_INFO(logger_, "Free Slot (%d , %lu) \n", slot_num, slots_[slot_num]);
        int idx = slot_num <= size_ ? 0 : ((slot_num - size_ - 1) /num_big_slots_  + 1 );
        num_slots_taken_[idx]--; 
        slots_[slot_num] = 0;
        file_slots_.erase(file_num);
    }
    CheckNumSlotsTaken();
}
    
uint64_t SstBitMap::GetSlotFileNum(int slot_num){
    std::unique_lock<std::mutex> lk{mu_};
    // why this assert here? what about big slots
    assert(slot_num <= size_);
    return slots_[slot_num];
}

int SstBitMap::GetFileSlotNum(uint64_t file_num){
    std::unique_lock<std::mutex> lk{mu_};
    assert(file_slots_.find(file_num) != file_slots_.end());
    return file_slots_[file_num];
}

void SstBitMap::TakeSlot(uint64_t file_num, int slot_num, int times) {
    std::unique_lock<std::mutex> lk{mu_};
    slots_[slot_num] = file_num;
    file_slots_[file_num] = slot_num;
    num_slots_taken_[times-1]++;

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
