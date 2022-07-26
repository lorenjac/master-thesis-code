#ifndef MIDAS_TX_HPP
#define MIDAS_TX_HPP

#include <string>        // std::string
#include <vector>        // std::vector
#include <unordered_map> // std::unordered_map
#include <atomic>        // std::atomic

#include "types.hpp"
#include "version.hpp"

namespace midas {
namespace detail {

class Transaction {
public:
    using this_type = Transaction;
    using key_type = std::string;
    using value_type = std::string;
    using ptr = std::shared_ptr<this_type>;

    struct Mod {
        enum class Kind {
            Update,
            Insert,
            Remove
        };

        Kind            code;
        Version::ptr    v_origin; // nullptr if code == Insert
        value_type      delta;    // empty if code == Remove
        Version::ptr    v_new;    // nullptr if code == Remove
    };

    using write_set_t = std::unordered_map<key_type, Mod>;
    using read_set_t = std::vector<Version::ptr>;

    enum status_code {
        ACTIVE,
        COMMITTED,
        FAILED
    };

    using status_type = std::atomic<status_code>;

private:
    id_type mId;
    stamp_type mBegin;
    stamp_type mEnd;
    status_type mStatus;
    write_set_t mChangeSet;
    read_set_t mReadSet;

public:
    Transaction(const id_type id, const stamp_type begin)
        : mId{id}
        , mBegin{begin}
        , mEnd{}
        , mStatus{ACTIVE}
        , mChangeSet{}
        , mReadSet{}
    {}

    // Transactions cannot be copied
    explicit Transaction(const this_type& other) = delete;
    this_type& operator=(const this_type& other) = delete;

    // Transactions could be moved there is currently no need for that
    explicit Transaction(this_type&& other) = delete;
    this_type& operator=(this_type&& other) = delete;

    // Destruction is trivial
    ~Transaction() = default;

    id_type getId() const { return mId; }
    stamp_type getBegin() const { return mBegin; }
    stamp_type getEnd() const { return mEnd; }
    const status_type& getStatus() const { return mStatus; }
    const write_set_t& getChangeSet() const { return mChangeSet; }
    const read_set_t& getReadSet() const { return mReadSet; }

    void setBegin(const stamp_type begin) { mBegin = begin; }
    void setEnd(const stamp_type end) { mEnd = end; }
    status_type& getStatus() { return mStatus; }
    write_set_t& getChangeSet() { return mChangeSet; }
    read_set_t& getReadSet() { return mReadSet; }

}; // end class transaction

} // end namespace detail
} // end namespace midas

#endif
