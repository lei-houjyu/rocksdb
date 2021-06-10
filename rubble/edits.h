#include <string>
#include <vector>
#include <mutex>

class Edits {

  public:
    Edits();

    void AddEdit(std::string edit);

    void GetEdits(std::vector<std::string>& edits);

    size_t size(){ return edits_.size();}

  private:
    std::vector<std::string> edits_;

    std::mutex mu_;
};