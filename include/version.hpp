#ifndef MIDAS_VERSION_HPP
#define MIDAS_VERSION_HPP

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/p.hpp>

#include "types.hpp"
#include "string.hpp"

#include <atomic>

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

struct Version {
    using ptr = pmdk::persistent_ptr<Version>;

    // timestamp from when this version was created (became visible)
    // TODO make atomic
    // TODO make persistent
    stamp_type begin;

    // timestamp from when this version was invalidated
    // TODO make persistent
    std::atomic<stamp_type> end;

    // payload of this version
    NVString data;

    Version()
        : begin{}
        , end{}
        , data{}
    {}
};

} // end namespace detail
} // end namespace midas

#endif
