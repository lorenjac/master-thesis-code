#ifndef HISTORY_HPP
#define HISTORY_HPP

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>

#include "types.hpp"
#include "list.hpp"
#include "version.hpp"

namespace midas {
namespace detail {

namespace pmdk = pmem::obj;

struct history {
    using ptr = pmdk::persistent_ptr<history>;
    using elem_type = version::ptr;

    util::list<elem_type> chain;

    // Synchronizes access to this history
    // Reset on restart!
    pmdk::mutex mutex;

    history()
        : chain{}
        , mutex{}
    {}
};

}
}

#endif
