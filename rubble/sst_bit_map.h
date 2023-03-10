#include <vector>
#include <mutex>
#include <atomic>
#include <set>
#include <unordered_map>
#include <rocksdb/env.h>
#include <condition_variable>

// a circular array implementation of bit map
class SstBitMap{
public:
    SstBitMap(int pool_size, int max_num_mems_in_flush,
    std::shared_ptr<rocksdb::Logger> logger = nullptr,
    std::shared_ptr<rocksdb::Logger> map_logger = nullptr, bool is_tail = false, bool is_primary = false);
    
    // take one slot for a specific file
    int TakeOneAvailableSlot(uint64_t file_num, int times);
    
    // free the slots occupied by the set of files
    void FreeSlot(std::set<uint64_t> file_nums);

    // free the slot occupied by a file and returns the occupied slot num
    int FreeSlot(uint64_t file_num);

    void FreeSlot2(int slot);

    bool CheckSlotFreed(int slot_num);

    // Get the file num that occupies the specific slot 
    uint64_t GetSlotFileNum(int slot_num);

    // Get the slot num occupied by a file
    int GetFileSlotNum(uint64_t file_num);

    // update sst bit map with file num and slot num
    void TakeSlot(uint64_t file_num, int slot_num, int times);

    void WaitForFreeSlot();

    void NotifyFreeSlot();

    bool IsFull();

private:
    // check if the total num of slots taken matches the size of file_slots_
    void CheckNumSlotsTaken();
  
    /* data */
    std::vector<int> next_available_slot_;

    std::vector<int> num_slots_taken_ ;

    // size of the slots of sst of normal size
    int size_;

    // number of slots for each size of sst which is multiple times as the normal size
    int num_big_slots_{20};

    int max_num_mems_in_flush_{0};

    //slots_[i] stores the file num that occupies slot i
    std::vector<uint64_t> slots_;

    // track whether the slot is still occupied in secondary and primary
    std::vector<int> slot_usage_;
    int slot_initial_usage_;

    // keep track of the slot num taken by a file  
    std::unordered_map<uint64_t, int> file_slots_;

    std::mutex mu_;

    std::shared_ptr<rocksdb::Logger> logger_;

    // log the operations on the map, including add and delete an entry
    std::shared_ptr<rocksdb::Logger> map_logger_;

    std::condition_variable bitmap_full_cond_;
    std::mutex bitmap_full_mu_;

    std::atomic_bool is_full_;
};