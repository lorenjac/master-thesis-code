#ifndef MIDAS_HPP
#define MIDAS_HPP

#include "store.hpp"

namespace midas {

    using detail::init;
    using detail::Store;
    using detail::Transaction;

    using pop_type = detail::Store::pool_type;
}

#endif
