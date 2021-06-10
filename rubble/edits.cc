#include "edits.h"

Edits::Edits(){ }

void Edits::AddEdit(std::string edit){
    std::unique_lock<std::mutex> lk{mu_};
    edits_.push_back(edit);
}

void Edits::GetEdits(std::vector<std::string>& edits){
    std::unique_lock<std::mutex> lk{mu_};
    edits_.swap(edits);
}
