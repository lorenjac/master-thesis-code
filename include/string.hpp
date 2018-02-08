#ifndef STRING_HPP
#define STRING_HPP

#include <stdexcept> // std::out_of_range
#include <ostream>   // std::ostream
#include <iostream>  // std::cout, ...
#include <cstddef>   // std::size_t
#include <utility>   // std::swap
#include <string>    // std::string

#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>

namespace util {

namespace pmdk = pmem::obj;

// TODO string: improve API
// * turn into class
// * terminate string with null-byte
// * add more functions from std::string
// * consider introducing a capacity to avoid needless allocations
struct string
{
// ############################################################################
// TYPES
// ############################################################################

    using this_type = string;
    using size_type = std::size_t;
    using volatile_string = std::string;

// ############################################################################
// MEMBER VARIABLES
// ############################################################################

    pmdk::persistent_ptr<char[]> data;
    pmdk::p<size_type> size;

// ############################################################################
// CONSTRUCTORS
// ############################################################################

    string()
        : data{}
        , size{}
    {}

    // Copying is not allowed at the moment
    // One reason is that I really want to avoid all kinds of allocations.
    // By prohibiting copying, I get a compiler error whenever someone tries.
    explicit string(const this_type& other) = delete;

    explicit string(this_type&& other)
        : data{}
        , size{}
    {
        std::cout << "WARNING: string::string(string&&) called!" << std::endl;
        std::swap(data, other.data);
        std::swap(size, other.size);
    }

    // Copying is not allowed at the moment
    // One reason is that I really want to avoid all kinds of allocations.
    // By prohibiting copying, I get a compiler error whenever someone tries.
    this_type& operator=(const this_type& other) = delete;

    this_type& operator=(this_type&& other)
    {
        std::cout << "WARNING: string::operator=(string&&) called!" << std::endl;
        std::swap(data, other.data);
        std::swap(size, other.size);
        return *this;
    }

    ~string()
    {
        pmdk::delete_persistent<char[]>(data, size);
    }

// ############################################################################
// API
// ############################################################################

    bool operator==(const this_type& other) const
    {
        if (size != other.size)
            return false;

        const auto numChars = size.get_ro();
        for (size_type i=0; i<numChars; ++i)
            if (data[i] != other[i])
                return false;
        return true;
    }

    bool operator!=(const this_type& other) const
    {
        return !(*this == other);
    }

    const char& at(const size_type pos) const
    {
        if (pos >= size.get_ro())
            throw std::out_of_range("string::at(): index is out of range!");

        return data[pos];
    }

    char& at(const size_type pos)
    {
        if (pos >= size.get_ro())
            throw std::out_of_range("string::at(): index is out of range!");

        return data[pos];
    }

    char& operator[](const size_type pos)
    {
        // no bounds checking as in std::string::operator[]
        return data[pos];
    }

    const char& operator[](const size_type pos) const
    {
        // no bounds checking as in std::string::operator[]
        return data[pos];
    }

    bool empty() const { return size.get_ro() == 0; }

    std::string to_std_string() const
    {
        if (empty())
            return {};

        return std::string(data.get(), size.get_ro());
    }

// ############################################################################
// Compatibility operators for hashmap
// ############################################################################

    bool operator==(const volatile_string& other) const
    {
        const auto numChars = size.get_ro();
        if (numChars != other.size())
            return false;

        for (size_type i=0; i<numChars; ++i)
            if (data[i] != other[i])
                return false;
        return true;
    }

    this_type& operator=(const volatile_string& other)
    {
        const auto otherSize = other.size();
        pmdk::delete_persistent<char[]>(data, size);
        data = pmdk::make_persistent<char[]>(otherSize);
        for (size_type i=0; i<otherSize; ++i)
            data[i] = other[i];
        size.get_rw() = other.size();
        return *this;
    }
};

std::ostream& operator<<(std::ostream& os, const string& str);

} // end namespace util

#endif
