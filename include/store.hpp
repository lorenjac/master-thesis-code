#ifndef STORE_HPP
#define STORE_HPP

#include <cstddef>
#include <string>

#include <libpmemobj++/pool.hpp>

#include <libcuckoo/cuckoohash_map.hh>

#include "hashmap.h"
#include "string.h"

namespace midas {

namespace pmdk = pmem::obj;

class root_t {};

class tx_t {
    using ptr = std::shared_ptr<tx_t>;
};

// ############################################################################
// Controls how volatile keys are mapped to persistent keys append ensures that
// both key types produce the same hashes (required for rehashing)
// ############################################################################

class hasher {
public:
    using volatile_key_type = std::string;
    using persistent_key_type = util::string;
    using result_type = std::size_t;

    static result_type hash(const volatile_key_type& key) {
        return _hash(key.data(), key.size());
    }

    static result_type hash(const persistent_key_type& key) {
        return _hash(key.data.get(), key.size);
    }

private:
    static result_type _hash(const char* str, result_type size) {
        // size_type hash = seed;
        result_type hash = 0;
        for (result_type i=0; i<size; ++i)
        {
            hash = hash * 101 + *str++;
        }
        return hash;
    }
};

// ############################################################################
// The type of the mapped values (arbitrary)
// ############################################################################

struct history {
    using elem_type = pm::persistent_ptr<int>;

    pm::p<util::list<elem_type>> chain;
    bool lock;

    history()
        : chain{}
        , lock{}
    {}
};

using value_type = history;

// ############################################################################
// Several parameters that control the behaviour of the hashmap (optional)
// ############################################################################

struct index_config {
    using size_type = util::hashmap_config::size_type;
    using float_type = util::hashmap_config::float_type;

    static constexpr size_type INIT_SIZE = 4;
    static constexpr size_type GROW_FACTOR = 2;
    static constexpr float_type MAX_LOAD_FACTOR = 0.75;
};

// ############################################################################
// Store
// ############################################################################

class store
{
// ############################################################################
// TYPES
// ############################################################################
public:
    using this_type = store;
    using key_type = std::string;
    using mapped_type = std::string;
    using size_type = std::size_t;

    using pool_type = pmdk::pool<root_t>;
    using tx_table_type = cuckoohash_map<size_type, tx_t::ptr>;
    using index_type = util::hashmap<hasher,
                                     pmdk::persistent_ptr<history>,
                                     index_config>;

// ############################################################################
// MEMBER VARIABLES
// ############################################################################

private:
    pool_type&      pop;
    index_type*     map;
    tx_table_type   txs;

// ############################################################################
// PUBLIC API
// ############################################################################

public:
    store(pool_type& pop)
        : pop{pop}
        , map{}
        , txs{}
    {}

    // Copying is not allowed
    store(const this_type& other) = delete;
    this_type& operator=(const this_type& other) = delete;

    // Moving is not allowed
    store(this_type&& other) = delete;
    this_type& operator=(this_type&& other) = delete;

    ~store() = default;

    bool empty() const;
    size_type size() const;

    tx_t::ptr begin();
    int abort(tx_t::ptr);
    int commit(tx_t::ptr);

    int insert(tx_t& tx, const key_type& key, const value_type& value);
    int get(tx_t& tx, const key_type& key, value_type& value);
    int drop(tx_t& tx, const key_type& key);

// ############################################################################
// PRIVATE API
// ############################################################################
};

}

#endif
