#ifndef INDEX_HPP
#define INDEX_HPP

#include <string> // std::string

#include "types.hpp"
#include "hashmap.hpp"
#include "string.hpp"

namespace midas {
namespace detail {
namespace index {

namespace pmdk = pmem::obj;

// ############################################################################
// Controls how volatile keys are mapped to persistent keys append ensures that
// both key types produce the same hashes (required for rehashing)
// ############################################################################

class hasher {
public:
    using volatile_key_type = std::string;
    using persistent_key_type = string;
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

struct config {
    using size_type = hashmap_config::size_type;
    using float_type = hashmap_config::float_type;

    static constexpr size_type INIT_SIZE = 4;
    static constexpr size_type GROW_FACTOR = 2;
    static constexpr float_type MAX_LOAD_FACTOR = 0.75;
};

} // end namespace index
} // end namespace detail
} // end namespace midas

#endif
