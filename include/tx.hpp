#ifndef TX_HPP
#define TX_HPP

#include <string>
// #include <vector> // std::vector
#include <unordered_map> // std::unordered_map
#include <atomic> // std::atomic

#include "types.hpp"
#include "version.hpp"

namespace midas {
namespace detail {

class transaction {
public:
    using this_type = transaction;
    using key_type = std::string;
    using value_type = std::string;
    using ptr = std::shared_ptr<this_type>;

    struct Mod {
        enum class Kind {
            Update,
            Insert,
            Remove
        };

        Kind                    code;
        version::ptr    v_origin; // nullptr if code == Insert
        value_type              delta;   // empty if code == Remove
        version::ptr    v_new; // nullptr if code == Remove
    };

    using delta_type = std::unordered_map<key_type, Mod>;

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
    delta_type mChangeSet;

public:
    transaction(const id_type id, const stamp_type begin)
        : mId{id}
        , mBegin{begin}
        , mEnd{}
        , mStatus{ACTIVE}
        , mChangeSet{}
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
    const status_type& getStatus() const { return mStatus; }
    const delta_type& getChangeSet() const { return mChangeSet; }

    void setBegin(const stamp_type begin) { mBegin = begin; }
    void setEnd(const stamp_type end) { mEnd = end; }
    status_type& getStatus() { return mStatus; }
    delta_type& getChangeSet() { return mChangeSet; }

}; // end class transaction

} // end namespace detail
} // end namespace midas

#endif
