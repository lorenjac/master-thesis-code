#ifndef MIDAS_STORE_HPP
#define MIDAS_STORE_HPP

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
namespace detail {

namespace pmdk = pmem::obj;

class Store
{
// ############################################################################
// TYPES
// ############################################################################

public:
    using this_type = Store;
    using key_type = std::string;
    using mapped_type = std::string;

    using tx_table_type = cuckoohash_map<id_type, Transaction::ptr>;
    using index_type = NVHashmap<IndexHasher, History::ptr, IndexParams>;

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
    explicit Store(pool_type& pop);

    // Copying is not allowed
    explicit Store(const this_type& other) = delete;
    this_type& operator=(const this_type& other) = delete;

    // Moving is not allowed
    explicit Store(this_type&& other) = delete;
    this_type& operator=(this_type&& other) = delete;

    ~Store() = default;

    Transaction::ptr begin();
    int abort(Transaction::ptr tx, int reason);
    int commit(Transaction::ptr tx);

    int read(Transaction::ptr tx, const key_type& key, mapped_type& result);
    int write(Transaction::ptr tx, const key_type& key, const mapped_type& value);
    int drop(Transaction::ptr tx, const key_type& key);

    void print();

// ############################################################################
// PRIVATE API
// ############################################################################

private:

    void init();
    void purgeHistory(History::ptr& history);

    int insert(Transaction::ptr tx, const key_type& key, const mapped_type& value);
    Version::ptr getWritableSnapshot(History::ptr& history, Transaction::ptr tx);
    Version::ptr getReadableSnapshot(History::ptr& history, Transaction::ptr tx);
    bool isWritable(Version::ptr& v, Transaction::ptr tx);
    bool isReadable(Version::ptr& v, Transaction::ptr tx);
    bool validate(Transaction::ptr tx);
    void rollback(Transaction::ptr tx);
    void finalize(Transaction::ptr tx);
    bool persist(Transaction::ptr tx);

    bool isValidTransaction(const Transaction::ptr tx);

    /**
     * Tests whether the given history contains at least one
     * version that is not permanently invalidated.
     */
    bool hasValidSnapshots(const History::ptr& hist);

    /**
     * Tests whether the given value is a transaction id.
     */
    inline bool isTransactionId(const stamp_type data);
};

bool init(Store::pool_type& pop, std::string file, size_type pool_size);

} // end namespace detail
} // end namespace midas

#endif
