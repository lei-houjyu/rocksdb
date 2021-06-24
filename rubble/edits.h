#include <string>
#include <vector>
#include <mutex>
#include <map>
#include <atomic>

class Edits {

  public:
    Edits();

    // used by the primary node
    void AddEdit(std::string edit);

    void GetEdits(std::vector<std::string>& edits);

    size_t size(){ return edits_.size();}

    // used by the non-primary nodes
    void Insert(uint64_t id, const std::string& edit);

    std::string PopFirst();

  private:
    std::vector<std::string> edits_;

    std::map<uint64_t, std::string> ordered_edits_;

    std::mutex mu_;
};