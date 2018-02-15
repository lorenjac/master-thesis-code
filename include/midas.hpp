#ifndef MIDAS_HPP
#define MIDAS_HPP

#include "store.hpp"

namespace midas {
    
    using detail::init;
    using detail::store;
    using detail::transaction;

    using pop_type = detail::store::pool_type;
}

#endif
