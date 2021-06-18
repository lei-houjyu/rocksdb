#include "edits.h"
#include <assert.h>

Edits::Edits(){ }

void Edits::AddEdit(std::string edit){
    std::unique_lock<std::mutex> lk{mu_};
    edits_.push_back(edit);
}

void Edits::GetEdits(std::vector<std::string>& edits){
    std::unique_lock<std::mutex> lk{mu_};
    edits_.swap(edits);
}


// used by the non-primary nodes
void Edits::Put(uint64_t id, const std::string& edit){
    std::unique_lock<std::mutex> lk{mu_};
    ordered_edits_.insert({id, edit});
}

std::string Edits::GetFirst(){
    std::unique_lock<std::mutex> lk{mu_};
    assert(ordered_edits_.size() >= 1);
    auto it = ordered_edits_.begin();
    auto edit = it->second;
    ordered_edits_.erase(it);
    return edit;
}