#ifndef MIDAS_HPP
#define MIDAS_HPP

#include "Store.hpp"

namespace midas {

    using detail::init;
    using detail::Store;
    using detail::transaction;

    using pop_type = detail::Store::pool_type;
}

#endif
