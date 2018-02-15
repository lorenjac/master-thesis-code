#ifndef VERSION_HPP
#define VERSION_HPP

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>

#include "types.hpp"
#include "string.hpp"

#include <atomic>

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

struct version {
    using ptr = pmdk::persistent_ptr<version>;

    // timestamp from when this version was created (became visible)
    // ATOMIC
    // PERSISTENT
    stamp_type begin;

    // timestamp from when this version was invalidated
    // ATOMIC
    // PERSISTENT
    // stamp_type end;
    std::atomic<stamp_type> end;

    // payload of this version
    string data;

    version()
        : begin{}
        , end{}
        , data{}
    {}
};

} // end namespace detail
} // end namespace midas

#endif
