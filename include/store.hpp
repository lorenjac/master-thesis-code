#ifndef STORE_HPP
#define STORE_HPP

#include <string>
#include <mutex>

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

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

    enum : stamp_type
    {
        TS_INFINITY = std::numeric_limits<stamp_type>::max() - 1,
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

    // Logical clock for handing out timestamps. Must be even.
    std::atomic<stamp_type> timestampCounter;

    // Pool for handing out unique transaction identifiers. Must be odd.
    std::atomic<id_type> idCounter;

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
