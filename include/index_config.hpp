#ifndef MIDAS_INDEX_HPP
#define MIDAS_INDEX_HPP

#include <string> // std::string

#include "types.hpp"
#include "hashmap.hpp"
#include "string.hpp"

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

// ############################################################################
// Controls how volatile keys are mapped to persistent keys append ensures that
// both key types produce the same hashes (required for rehashing)
// ############################################################################

class IndexHasher {
public:
    using volatile_key_type = std::string;
    using persistent_key_type = NVString;
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
// Several parameters that control the behaviour of the hashmap (optional)
// ############################################################################

struct IndexParams {
    using size_type = DefaultHashmapConfig::size_type;
    using float_type = DefaultHashmapConfig::float_type;

    static constexpr size_type INIT_SIZE = 4;
    static constexpr size_type GROW_FACTOR = 2;
    static constexpr float_type MAX_LOAD_FACTOR = 0.75;
};

} // end namespace detail
} // end namespace midas

#endif
