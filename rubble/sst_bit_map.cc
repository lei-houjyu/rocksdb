#include "sst_bit_map.h"
#include <iostream>
#include <assert.h>

SstBitMap::SstBitMap(int pool_size)
    :size_(pool_size){
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
        std::cerr << "slot " <<  slot_num << " already taken by " << slots_.at(slot_num) << std::endl;
        std::cerr << "total " << num_slots_taken_ << " slots already taken\n";
        assert(false);
    }
    
    slots_[slot_num] = file_num;
    file_slots_.emplace(file_num, slot_num);
    // std::cout << "file " << file_num << " took slot " << next_available_slot_ << std::endl;
    num_slots_taken_++;
    if(slot_num == size_){
        next_available_slot_.store(1);
    }else{
        next_available_slot_.fetch_add(1);
    }

    return slot_num;
}

int SstBitMap::FreeSlot(uint64_t file_num){
    std::unique_lock<std::mutex> lk{mu_};
    int slot_num = 0;
    int i = 1;
    for(; i <= size_; i++){
        if(slots_[i] == file_num){
            slot_num = i;
            num_slots_taken_--;
            slots_[i] = 0;
            file_slots_.erase(file_num);
            break;
        }
    }
    assert(i != (size_ + 1));
    return slot_num;
}

void SstBitMap::FreeSlot(std::set<uint64_t> file_nums){
    std::unique_lock<std::mutex> lk{mu_};
    int num_slots_to_free = file_nums.size();
    int num_slot_freed = 0;
    
    std::cout << "Free :";
    for(int i = 1 ; i <= size_; i++){
        if(file_nums.find(slots_[i]) != file_nums.end()){
            assert(slots_[i] != 0);
            std::cout << " (" << i << "," << slots_[i] << ")";
            uint64_t file_num = slots_[i];
            file_slots_.erase(file_num);
            slots_[i] = 0;
            num_slot_freed++;
            num_slots_taken_--;
            if(num_slot_freed == num_slots_to_free){
                break;
            }
        }
    }
    std::cout << '\n';
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
