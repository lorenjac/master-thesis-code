#ifndef MIDAS_HISTORY_HPP
#define MIDAS_HISTORY_HPP

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>

#include "types.hpp"
#include "list.hpp"
#include "version.hpp"

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

struct History {
    using ptr = pmdk::persistent_ptr<History>;
    using elem_type = Version::ptr;

    NVList<elem_type> chain;

    // Synchronizes access to this history
    // Reset on restart!
    pmdk::mutex mutex;

    History()
        : chain{}
        , mutex{}
    {}
};

} // end namespace detail
} // end namespace midas

#endif
