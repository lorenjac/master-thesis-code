#ifndef MIDAS_HASHMAP_HPP
#define MIDAS_HASHMAP_HPP

#include <cstddef>   // std::size_t
#include <utility>   // std::swap
#include <iostream>  // std::cout, std::endl (debugging)

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/p.hpp>

#include "list.hpp"
#include "string.hpp"

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

struct DefaultHashmapConfig {
    using size_type = std::size_t;
    using float_type = double;

    static constexpr size_type INIT_SIZE = 64;
    static constexpr size_type GROW_FACTOR = 2;
    static constexpr float_type MAX_LOAD_FACTOR = 0.75;
};

template <class Hash, class T, class Config = DefaultHashmapConfig>
class NVHashmap
{

// ############################################################################
// TYPES
// ############################################################################

public:
    using hash_type = Hash;
    using mapped_type = T;
    using size_type = std::size_t;
    using this_type = NVHashmap<Hash, T, Config>;

    // Keys of this type are only used for queries but are never stored.
    // When storing keys, volatile keys are copied into persistent keys.
    // As a result, retrieving a pair will reveal a different key type.
    using volatile_key = typename hash_type::volatile_key_type;

    // Keys of this type are only used for storage.
    // This type is only revealed when retrieving pairs.
    using persistent_key = typename hash_type::persistent_key_type;

    class iterator;

    // A persistent key-value pair.
    // The type of values is arbitrary but it is strongly recommended to use
    // pmdk::p<X> for primitives and pmdk::persistent_ptr<X> for classes/PODs.
    struct pair
    {
        pair()
            : key{}
            , value{}
        {}

        pmdk::p<persistent_key> key;
        mapped_type value;
    };

    // Each bucket stores key-value pairs of equally-hashing keys.
    // In this case, each bucket is simply a list of pair pointers.
    using bucket_type = NVList<pmdk::persistent_ptr<pair>>;

// ############################################################################
// MEMBER VARIABLES
// ############################################################################

private:
    pmdk::persistent_ptr<bucket_type[]> mBuckets; // holds buckets of this table
    pmdk::p<size_type> mBucketCount; // number of buckets in this table
    pmdk::p<size_type> mElemCount; // number of elements in this table

// ############################################################################
// PUBLIC API
// ############################################################################

public:
    NVHashmap()
        : mBuckets{}
        , mBucketCount{}
        , mElemCount{}
    {}

    NVHashmap(const this_type& other) = delete;

    NVHashmap(this_type&& other)
        : mBuckets{other.mBuckets}
        , mBucketCount{other.mBucketCount}
        , mElemCount{other.mElemCount}
    {
        other.mBuckets = nullptr;
        other.mBucketCount.get_rw() = 0;
        other.mElemCount.get_rw() = 0;
    }

    ~NVHashmap()
    {
        pmdk::delete_persistent<bucket_type[]>(mBuckets, mBucketCount);
    }

    this_type& operator=(const this_type& other) = delete;

    this_type& operator=(this_type&& other)
    {
        std::swap(mBuckets, other.mBuckets);
        std::swap(mBucketCount, other.mBucketCount);
        std::swap(mElemCount, other.mElemCount);
        return *this;
    }

    /**
     * Inserts a key-value pair.
     *
     * If there exists a pair with the same key then this function has no effect
     * and returns false.
     *
     * Allocates a initial table if none was created during construction.
     *
     * Triggers an expansion when the maximum load factor is exceeded.
     *
     * Returns true if the given pair was inserted successfully.
     */
    template <class pool_type>
    bool put(const volatile_key& key, const mapped_type& value,
             pmdk::pool<pool_type>& pool)
    {
        // Allocate the table if there are no buckets yet
        if (!mBuckets) {
            pmdk::transaction::exec_tx(pool, [&,this](){
                mBuckets =
                    pmdk::make_persistent<bucket_type[]>(Config::INIT_SIZE);
                mBucketCount.get_rw() = Config::INIT_SIZE;
            });
        }

        // Get bucket
        auto& bucket = mBuckets[hash(key)];

        // Return if the bucket contains a pair with the same key
        for (const auto& elem : bucket)
            if (elem->key.get_ro() == key)
                return false;

        // Insert new elem at the back of the bucket
        pmdk::transaction::exec_tx(pool, [&,this](){

            // Create new pair
            const auto new_pair = pmdk::make_persistent<pair>();

            // Convert volatile key to persistent key and store in pair
            new_pair->key.get_rw() = key;
            new_pair->value = value;

            // Add the new pair to the bucket
            bucket.push_back(new_pair, pool);
            ++mElemCount.get_rw();
        });

        // Expand table when maximum load factor is exceeded
        if (load() > Config::MAX_LOAD_FACTOR)
            grow(Config::GROW_FACTOR, pool);

        return true;
    }

    /**
     * Retrieves the value for a given key.
     *
     * If there exists no pair with the same key then this function has no
     * effect and returns false.
     *
     * If no table has been allocated before, then this method has no effect
     * and returns false.
     *
     * Returns true if the given pair was found and stores the mapped value
     * in the output parameter.
     */
    bool get(const volatile_key& key, mapped_type& value) const
    {
        // Return if there are no buckets yet
        if (!mBuckets)
            return false;

        // Get bucket
        auto& bucket = mBuckets[hash(key)];

        // Find pair with matching key and store its value in output parameter
        for (const auto& elem : bucket) {
            if (elem->key.get_ro() == key) {
                value = elem->value;
                return true;
            }
        }
        return false;
    }

    /**
     * Removes a key-value pair with the given key (if any).
     *
     * If there exists no pair with the same key then this function has no
     * effect and returns false.
     *
     * If no table has been allocated before, then this method has no effect
     * and returns false.
     *
     * Returns true if the given pair was removed successfully.
     */
    template <class pool_type>
    bool erase(const volatile_key& key, pmdk::pool<pool_type>& pool)
    {
        // Return if there are no buckets yet
        if (!mBuckets)
            return false;

        // Get bucket
        auto& bucket = mBuckets[hash(key)];

        // Find and remove pair with the given key
        const auto end = bucket.end();
        for (auto it = bucket.begin(); it != end; ++it) {
            if ((*it)->key.get_ro() == key) {
                pmdk::transaction::exec_tx(pool, [&,this](){
                    bucket.erase(it, pool);
                    --mElemCount.get_rw();
                });
                return true;
            }
        }
        return false;
    }

    /**
     * Removes key-value pair at the position of the iterator.
     *
     * If the iterator is valid, then the addressed key-value pair is removed
     * and an iterator to the next element (if any) is returned. Otherwise,
     * this method has no effect and returns the given iterator unaltered.
     *
     * Returns incremented iterator if iterator is valid, identity otherwise.
     */
    template <class pool_type>
    iterator erase(iterator& it, pmdk::pool<pool_type>& pool)
    {
        // Return if there are no buckets yet
        if (it == end())
            return it;

        // Get info to locate current bucket item
        auto bucket_it = it.bucket_iter;
        auto table_idx = it.table_index;

        // Go to next bucket item to avoid invalidation and provide the caller
        // with a usable iterator to proceed. Still, the iterator could be at
        // its end, so the caller is in charge of testing for end().
        ++it;

        // Remove previous bucket item
        auto& bucket = mBuckets[table_idx];
        pmdk::transaction::exec_tx(pool, [&,this](){
            bucket.erase(bucket_it, pool);
            --mElemCount.get_rw();
        });
        return it;
    }

    /**
     * Removes all key-value pairs from this table but keeps the
     * table and its empty buckets.
     *
     * Does nothing if the number of buckets or the number of items is zero.
     */
    template <class pool_type>
    void clear(pmdk::pool<pool_type>& pool)
    {
        const auto numBuckets = mBucketCount.get_ro();
        const auto numElems = mElemCount.get_ro();

        // Return if there are no buckets or not pairs
        if (numBuckets == 0 || numElems == 0)
            return;

        pmdk::transaction::exec_tx(pool, [&,this](){
            for (size_type i=0; i<numBuckets; ++i)
                if (!mBuckets[i].empty())
                    mBuckets[i].clear(pool);
            mElemCount.get_rw() = 0;
        });
    }

    /** Returns the number of buckets in this table */
    size_type buckets() const { return mBucketCount; }

    /** Returns the number of elements in this table */
    size_type size() const { return mElemCount; }

    /** Tests whether the table has no elements */
    bool empty() const { return mElemCount.get_ro() == 0; }

    void show(bool showEmptyBuckets) const
    {
        const auto numBuckets = mBucketCount.get_ro();
        for (size_type i=0; i<numBuckets; ++i) {
            if (!showEmptyBuckets && mBuckets[i].empty())
                continue;
            std::cout << "bucket[" << i << "]:\n";
            size_type j = 0;
            for (const auto& pair_ptr : mBuckets[i]) {
                if (j++ != 0) {
                    std::cout << ",\n";
                }
                std::cout << "  " << pair_ptr->key.get_ro();
                std::cout << " -> " << pair_ptr->value;
            }
            std::cout << std::endl;
        }
    }

    /**
     * Non-const iterator
     */
    class iterator
    {
        friend this_type;

    public:
        using elem_type = typename bucket_type::elem_type;

    private:
        pmdk::persistent_ptr<bucket_type[]> table;
        size_type table_size;

        size_type table_index;
        typename bucket_type::iterator bucket_iter;
        typename bucket_type::iterator bucket_end;

    public:
        iterator()
            : table(nullptr)
            , table_size(0)
            , table_index(0)
            , bucket_iter{}
            , bucket_end{}
        {}

        iterator(pmdk::persistent_ptr<bucket_type[]> table,
                size_type table_size)
            : table(table)
            , table_size(table_size)
            , table_index(0)
            , bucket_iter{}
            , bucket_end{}
        {
            seek();
        }

        void seek()
        {
            for (; table_index < table_size; ++table_index) {
                auto& bucket = table[table_index];
                if (bucket.size()) {
                    bucket_iter = bucket.begin();
                    bucket_end = bucket.end();
                    break;
                }
            }
        }

        elem_type& operator*()
        {
            return *bucket_iter;
        }

        // elem_type* operator->()
        // {
        //     return bucket_iter;
        // }

        bool operator==(const iterator& other)
        {
            return bucket_iter == other.bucket_iter;
        }

        bool operator!=(const iterator& other) { return !(*this == other); }

        iterator operator++(int) {
            auto old = *this;
            ++(*this);
            return old;
        }

        iterator operator++() {
            // Go to next item. Needs no checking because bucket_iter is
            // invalid if and only if this iterator is invalid. In other words,
            // if this iterator is not equal to end() this operation must
            // succeed. Otherwise, behaviour is undefined.
            ++bucket_iter;

            // Since bucket_iter is only invalid when this iterator is invalid,
            // we must find another non-empty bucket or, else, this iterator
            // becomes invalid.
            if (bucket_iter == bucket_end) {
                // Starting from the next bucket search for a non-empty bucket.
                // I may be going out of bounds here but seek() can handle
                // that. Also, if seek() cannot find another non-empty bucket
                // then bucket_iter == bucket_end still holds which makes this
                // iterator compare positive against end(). If so, then this
                // iterator is invalid and incrementing yields undefined
                // behaviour.
                ++table_index;
                seek();
            }
            return *this;
        }
    };

    iterator begin() { return iterator(mBuckets, mBucketCount); }
    iterator end() { return iterator(); }

// ############################################################################
// PRIVATE API
// ############################################################################

private:
    size_type hash(const volatile_key& key) const {
        return hash_type::hash(key) % mBucketCount.get_ro();
    }

    size_type hash(const persistent_key& key, const size_type modulo) const {
        return hash_type::hash(key) % modulo;
    }

    /**
     * Computes the current load factor
     */
    double load() const
    {
        return static_cast<double>(size()) / buckets();
    }

    /**
     * Increases the size of the table by the given factor.
     */
    template <class pool_type>
    void grow(const size_type factor, pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            // Create new table
            const auto bucket_count_new = factor * mBucketCount;
            const auto buckets_new =
                    pmdk::make_persistent<bucket_type[]>(bucket_count_new);

            // Hash all elements into the new table
            rehash_to_dest(buckets_new, bucket_count_new, pool);

            // Install new table and delete old one
            pmdk::delete_persistent<bucket_type[]>(mBuckets, mBucketCount);
            mBuckets = buckets_new;
            mBucketCount.get_rw() = bucket_count_new;
        });
    }

    /**
     * Rehashes all elements from the current table into another table.
     */
    template <class pool_type>
    void rehash_to_dest(pmdk::persistent_ptr<bucket_type[]> dest,
                        const size_type dest_size, pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            const auto numBuckets = mBucketCount.get_ro();
            for (size_type i = 0; i < numBuckets; ++i) {
                auto& bucket = mBuckets[i];
                while (!bucket.empty()) {
                    auto new_pos = hash(bucket.get(0)->key.get_ro(), dest_size);
                    dest[new_pos].push_back_from(bucket, 0, pool);
                }
            }
        });
    }
}; // end class NVHashmap

} // end namespace detail
} // end namespace midas

#endif
