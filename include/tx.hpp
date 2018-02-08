#ifndef TX_HPP
#define TX_HPP

#include <utility>
#include <vector>
#include <atomic>

#include "types.hpp"
#include "version.hpp"

namespace midas {

class transaction {
public:
    using this_type = transaction;
    using ptr = std::shared_ptr<this_type>;

    using version = detail::version;
    using version_delta = std::pair<version::ptr, version::ptr>;
    using index_update = std::pair<std::string, version::ptr>;

    enum status {
        ACTIVE,
        COMMITTED,
        FAILED
    };

private:
    id_type mId;
    stamp_type mBegin;
    stamp_type mEnd;
    std::atomic<status> mStatus;

    std::vector<void*> mReadSet;
    std::vector<version_delta> mWriteSet;
    std::vector<index_update> mCreateSet;
    std::vector<index_update> mRemoveSet;

public:
    transaction(const id_type id, const stamp_type begin)
        : mId{id}
        , mBegin{begin}
        , mEnd{}
        , mStatus{ACTIVE}
        , mReadSet{}
        , mWriteSet{}
        , mCreateSet{}
        , mRemoveSet{}
    {}

    // Transactions cannot be copied
    explicit transaction(const this_type& other) = delete;
    this_type& operator=(const this_type& other) = delete;

    // Transactions could be moved there is currently no need for that
    explicit transaction(this_type&& other) = delete;
    this_type& operator=(this_type&& other) = delete;

    // Destruction is trivial
    ~transaction() = default;

    id_type getId() const { return mId; }
    stamp_type getBegin() const { return mBegin; }
    stamp_type getEnd() const { return mEnd; }

    void setId(const id_type id) { mId = id; }
    void setBegin(const stamp_type begin) { mBegin = begin; }
    void setEnd(const stamp_type end) { mEnd = end; }

    std::atomic<status>& getStatus() { return mStatus; }
    std::vector<void*>& getReadSet() { return mReadSet; }
    std::vector<version_delta>& getWriteSet() { return mWriteSet; }
    std::vector<index_update>& getCreateSet() { return mCreateSet; }
    std::vector<index_update>& getRemoveSet() { return mRemoveSet; }

    const std::atomic<status>& getStatus() const { return mStatus; }
    const std::vector<void*>& getReadSet() const { return mReadSet; }
    const std::vector<version_delta>& getWriteSet() const { return mWriteSet; }
    const std::vector<index_update>& getCreateSet() const { return mCreateSet; }
    const std::vector<index_update>& getRemoveSet() const { return mRemoveSet; }

}; // end class transaction

} // end namespace midas

#endif
