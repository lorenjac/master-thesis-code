#ifndef STORE_HPP
#define STORE_HPP

#include <cstddef>
#include <string>
#include <mutex>

#include <libpmemobj++/pool.hpp>

#include <libcuckoo/cuckoohash_map.hh>

#include "types.hpp"
#include "index_config.hpp"
#include "history.hpp"
#include "tx.hpp"

namespace midas {

namespace pmdk = pmem::obj;

class store
{
// ############################################################################
// TYPES
// ############################################################################

public:
    using this_type = store;
    using key_type = std::string;
    using mapped_type = std::string;

    using tx_table_type = cuckoohash_map<id_type, transaction::ptr>;
    using index_type = util::hashmap<detail::index::hasher,
                                     detail::history::ptr,
                                     detail::index::config>;

    struct root {
        pmdk::persistent_ptr<index_type> index;
    };
    using pool_type = pmdk::pool<root>;

    // Status codes of API calls
    enum {
        OK = 0,
        INVALID_TX,
        KEY_EXISTS,
        WRITE_CONFLICT,
        VALUE_NOT_FOUND = 404
    };

    /**
     * Infinity timestamp. Used in end timestamp fields to mark versions as valid.
     */
    enum : stamp_type
    {
        INFINITY = std::numeric_limits<stamp_type>::max() - 1,
        TS_DELTA = 2,
        TS_START = 2,
        ID_START = 1,
        TS_ZERO = 0
    };

// ############################################################################
// MEMBER VARIABLES
// ############################################################################

private:
    // Persistent object pool
    pool_type&      pop;

    // Index with mutex
    index_type*     index;
    std::mutex      index_mutex;

    // Transaction table
    tx_table_type   tx_tab;

    /**
     * Logical clock for handing out timestamps.
     *
     * This timer is initialized with two and is always incremented by two.
     * Therefore, timestamps are always even numbers. Transaction IDs on the
     * other hand are odd. This way it is easy to tell both from another when
     * inspecting begin/end fields of versions. We only need to test the LSB.
     *
     * Note: The zero timestamp is reserved for making versions invisible to
     * everyone (e.g. when rolling back inserts), so it starts at 2.
     */
    std::atomic<stamp_type> next_ts;

    /**
     * Pool for unique transaction identifiers.
     *
     * Initialized with one and always incremented by two. Always yields odd
     * numbers which can be easily differentiated from timestamps which are
     * always even.
     */
    std::atomic<id_type> next_id;

// ############################################################################
// PUBLIC API
// ############################################################################

public:
    explicit store(pool_type& pop);

    // Copying is not allowed
    explicit store(const this_type& other) = delete;
    this_type& operator=(const this_type& other) = delete;

    // Moving is not allowed
    explicit store(this_type&& other) = delete;
    this_type& operator=(this_type&& other) = delete;

    ~store() = default;

    transaction::ptr begin();
    int abort(transaction::ptr tx, int reason);
    int commit(transaction::ptr tx);

    int read(transaction::ptr tx, const key_type& key, mapped_type& result);
    int write(transaction::ptr tx, const key_type& key, const mapped_type& value);
    int drop(transaction::ptr tx, const key_type& key);

    void print();

// ############################################################################
// PRIVATE API
// ############################################################################

private:

    void init();
    void purgeHistory(detail::history::ptr& history);

    int insert(transaction::ptr tx, const key_type& key, const mapped_type& value);
    detail::version::ptr getWritableSnapshot(detail::history::ptr& history, transaction::ptr tx);
    detail::version::ptr getReadableSnapshot(detail::history::ptr& history, transaction::ptr tx);
    bool isWritable(detail::version::ptr& v, transaction::ptr tx);
    bool isReadable(detail::version::ptr& v, transaction::ptr tx);
    bool validate(transaction::ptr tx);
    void rollback(transaction::ptr tx);
    void finalize(transaction::ptr tx);
    bool persist(transaction::ptr tx);

    bool isValidTransaction(const transaction::ptr tx);

    /**
     * Tests whether the given history contains at least one
     * version that is not permanently invalidated.
     */
    bool hasValidSnapshots(const detail::history::ptr& hist);

    /**
     * Tests whether the given value is a transaction id.
     */
    inline bool isTransactionId(const stamp_type data);
};

}

#endif
