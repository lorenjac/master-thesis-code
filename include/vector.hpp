#ifndef VECTOR_HPP
#define VECTOR_HPP

#include <stdexcept> // std::out_of_range

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/p.hpp>

namespace util {

namespace pmdk = pmem::obj;

template <class T>
class vector
{
// ############################################################################
// TYPES
// ############################################################################

public:
    using elem_type = T;
    using size_type = std::size_t;
    using this_type = vector<elem_type>;

// ############################################################################
// MEMBER VARIABLES
// ############################################################################

private:
    pmdk::persistent_ptr<elem_type[]> mData;
    pmdk::p<size_type> mCapacity;
    pmdk::p<size_type> mSize;

// ############################################################################
// PUBLIC API
// ############################################################################

public:
    vector()
        : mData{}
        , mCapacity{}
        , mSize{}
    {}

    vector(const this_type& other)
        : mData(nullptr)
        , mCapacity(other.mCapacity)
        , mSize(other.mSize)
    {
        // Allocate memory if other vector appears to have some
        // Also copy all data of other vector (if any)
        if (other.mData) {
            mData = pmdk::make_persistent<elem_type[]>(capacity);
            for (size_type pos = 0; pos < mSize; ++pos)
                mData[pos] = other.mData[pos];
        }
    }

    vector(this_type&& other)
        : mData(other.mData)
        , mCapacity(other.mCapacity)
        , mSize(other.mSize)
    {
        other.mData = nullptr;
        other.mCapacity = 0;
        other.mSize = 0;
    }

    ~vector()
    {
        pmdk::delete_persistent<elem_type[]>(mData, mCapacity);
    }

    template <class pool_type>
    void init(pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            mData = nullptr;
            mCapacity = 0;
            mSize = 0;
        });
    }

    template <class pool_type>
    void push_back(const elem_type& elem, pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            if (mSize >= mCapacity)
                _expand();

            mData[mSize] = elem;
            mSize = mSize + 1;
        });
    }

    const elem_type& at(const size_type pos) const
    {
        if (pos >= mSize)
            throw std::out_of_range("vector::at(): index is out of range");

        return mData[pos];
    }

    elem_type& operator[](const size_type pos)
    {
        if (pos >= mSize)
            throw std::out_of_range("vector::operator[](): index is out of range");

        return mData[pos];
    }

    template <class pool_type>
    void erase(const size_type pos, pmdk::pool<pool_type>& pool)
    {
        if (pos >= mSize)
            throw std::out_of_range("vector::erase(): index is out of range");

        pmdk::transaction::exec_tx(pool, [&,this](){
            const size_type end = mSize - 1;
            for (size_type i=pos; i<end; ++i)
                mData[i] = mData[i + 1];

            mSize = mSize - 1;
        });
    }

    template <class pool_type>
    void clear(pmdk::pool<pool_type>& pool)
    {
        pmdk::transaction::exec_tx(pool, [&,this](){
            mSize = 0;
        });
    }

    template <class pool_type>
    void reserve(const size_type capacity, pmdk::pool<pool_type>& pool)
    {
        if (capacity <= mCapacity)
            return;

        pmdk::transaction::exec_tx(pool, [&,this](){
            _reserve(capacity);
        });
    }

    size_type capacity() { return mCapacity; }
    size_type size() { return mSize; }
    bool empty() { return mSize == 0; }

// ############################################################################
// PRIVATE API
// ############################################################################

private:
    void _reserve(const size_type capacity)
    {
        // Allocate array with new capacity
        auto newData = pmdk::make_persistent<elem_type[]>(capacity);

        // Copy old data to new array (if any)
        // Here we must work with current size because reserve() might get
        // called when size < capacity but we only want to copy actual items.
        for (size_type pos = 0; pos < mSize; ++pos)
            newData[pos] = mData[pos];

        // Release old array (mData may be nullptr)
        // Here we must the old capacity because we must release all memory of
        // the underlying array, even if some of it is unused (size < capacity)
        pmdk::delete_persistent<elem_type[]>(mData, mCapacity);

        // Install ptr to new array
        mData = newData;

        // Update capacity
        mCapacity = capacity;
    }

    void _expand()
    {
        // Compute new capacity
        size_type capacity = mCapacity;
        if (capacity)
            capacity *= 2;
        else
            capacity = 10;

        // Reallocate
        _reserve(capacity);
    }
};

} // end namespace util

#endif
