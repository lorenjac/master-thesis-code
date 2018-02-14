#ifndef STORE_INIT_HPP
#define STORE_INIT_HPP

#include <experimental/filesystem>
#include <iostream>
#include <string>

#include "store.hpp"

namespace midas {

    namespace fs = std::experimental::filesystem::v1;
    namespace pm = pmem::obj;

    using pool_type = midas::store::pool_type;
    using index_type = midas::store::index_type;
    using size_type = midas::size_type;

    bool init(pool_type& pop, std::string file, size_type pool_size)
    {
        const std::string layout{"index"};
        if (fs::exists(file)) {
            if (pool_type::check(file, layout) != 1) {
                std::cout << "File seems to be corrupt! Aborting..." << std::endl;
                return false;
            }
            pop = pool_type::open(file, layout);
        }
        else {
            pop = pool_type::create(file, layout, pool_size);
            auto root = pop.get_root();
            pm::transaction::exec_tx(pop, [&](){
                root->index = pm::make_persistent<index_type>();
            });
        }
        return true;
    }
}

#endif
