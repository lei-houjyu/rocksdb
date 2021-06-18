#include "sst_bit_map.h"
#include <iostream>
#include <assert.h>
#include <logging/logging.h>

SstBitMap::SstBitMap(int pool_size, std::shared_ptr<rocksdb::Logger> logger)
    :size_(pool_size), logger_(logger){
        slots_.reserve(size_ + 1);
        slots_.assign(size_ + 1, 0);
    }

int SstBitMap::TakeOneAvailableSlot(uint64_t file_num){
    std::unique_lock<std::mutex> lk{mu_};
    if(num_slots_taken_ == size_){
        std::cerr << "run out of slots, please choose a larger pool size\n";
        assert(false);
    }

    int slot_num = next_available_slot_.load();
    if(slots_[slot_num] != 0){
        int start = slot_num + 1;
        while(start <= size_ && slots_[start] != 0){
            start++;
        }
        while(start > size_ && ((start % size_) < slot_num) && (slots_[start & size_] != 0)){
            start++;
        }
        if(start > size_ && ((start % size_) == slot_num)){
            std::cerr << "total " << num_slots_taken_ << " slots already taken\n";
            assert(false);
        }
    
        slot_num = start > size_ ? start % size_ : start;
    }
    
    slots_[slot_num] = file_num;
    file_slots_.emplace(file_num, slot_num);
    // std::cout << "file " << file_num << " took slot " << next_available_slot_ << std::endl;
    num_slots_taken_++;
    // std::cout << "num slots taken : " << num_slots_taken_ << ", file_slots size : " << file_slots_.size() << std::endl;
    assert(static_cast<int>(file_slots_.size()) == num_slots_taken_);
    if(slot_num == size_){
        next_available_slot_.store(1);
    }else{
        next_available_slot_.store(slot_num + 1);
    }

    return slot_num;
}

int SstBitMap::FreeSlot(uint64_t file_num){
    std::unique_lock<std::mutex> lk{mu_};
    assert(file_slots_.find(file_num) != file_slots_.end());
    int slot_num = file_slots_[file_num];
    num_slots_taken_--;
    slots_[slot_num] = 0;
    file_slots_.erase(file_num);
    return slot_num;
}

void SstBitMap::FreeSlot(std::set<uint64_t> file_nums){
    std::unique_lock<std::mutex> lk{mu_};
    for(uint64_t file_num : file_nums){
        assert(file_slots_.find(file_num) != file_slots_.end());
        int slot_num = file_slots_[file_num];
        RUBBLE_LOG_INFO(logger_, "Free Slot (%d , %lu) \n", slot_num, slots_[slot_num]);
        num_slots_taken_--;
        slots_[slot_num] = 0;
        file_slots_.erase(file_num);
    }
    assert(static_cast<int>(file_slots_.size()) == num_slots_taken_);
}
    
uint64_t SstBitMap::GetSlotFileNum(int slot_num){
    std::unique_lock<std::mutex> lk{mu_};
    assert(slot_num <= size_);
    return slots_[slot_num];
}

int SstBitMap::GetFileSlotNum(uint64_t file_num){
    std::unique_lock<std::mutex> lk{mu_};
    assert(file_slots_.find(file_num) != file_slots_.end());
    return file_slots_[file_num];
}
