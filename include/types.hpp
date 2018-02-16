#ifndef MIDAS_TYPES_HPP
#define MIDAS_TYPES_HPP

#include <cstddef> // std::size_t
#include <cstdint> // std::uint64_t

namespace midas {
namespace detail {

    using size_type = std::size_t;
    using stamp_type = std::uint64_t;
    using id_type = stamp_type;

} // end namespace detail
} // end namespace midas

#endif
