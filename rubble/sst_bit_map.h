#include <vector>
#include <mutex>
#include <atomic>
#include <set>
#include <unordered_map>

// a circular array implementation of bit map
class SstBitMap{
public:
    SstBitMap(int pool_size);
    
    // take one slot for a specific file
    int TakeOneAvailableSlot(uint64_t file_num);
    
    // free the slots occupied by the set of files
    void FreeSlot(std::set<uint64_t> file_nums);

    // free the slot occupied by a file and returns the occupied slot num
    int FreeSlot(uint64_t file_num);

    // Get the file num that occupies the specific slot 
    uint64_t GetSlotFileNum(int slot_num);

    // Get the slot num occupied by a file
    int GetFileSlotNum(uint64_t file_num);

private:
    /* data */
    std::atomic<int> next_available_slot_{1};

    int num_slots_taken_;

    int size_;

    //slots_[i] stores the file num that occupies slot i
    std::vector<uint64_t> slots_;

    // keep track of the slot num taken by a file  
    std::unordered_map<uint64_t, int> file_slots_;

    std::mutex mu_;
};