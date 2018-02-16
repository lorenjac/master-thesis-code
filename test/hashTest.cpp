#include <experimental/filesystem>
#include <iostream>
#include <string>

#include "hashmap.hpp"
#include "string.hpp"
#include "list.hpp"

namespace fs = std::experimental::filesystem::v1;
namespace pm = pmem::obj;

namespace midas {
namespace detail {

std::ostream& operator<<(std::ostream& os, const string& str)
{
    const auto size = str.size.get_ro();
    os << "persistent_string [size=" << size;
    os << ", data={";
    for (string::size_type i=0; i<size; ++i)
        os << str.data[i];
    os << "}]";
    return os;
}

}
}

namespace app {

    using midas::detail::string;
    using midas::detail::NVHashmap;
    using midas::detail::DefaultHashmapConfig;

// ############################################################################
// Controls how volatile keys are mapped to persistent keys append ensures that
// both key types produce the same hashes (required for rehashing)
// ############################################################################

class my_string_hasher {
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
// The type of the mapped values (arbitrary)
// ############################################################################

// struct history {
//     using elem_type = pm::persistent_ptr<int>;
//
//     pm::p<list<elem_type>> chain;
//     bool lock;
//
//     history()
//         : chain{}
//         , lock{}
//     {}
// };
//
// using value_type = history;
using value_type = int;

// ############################################################################
// Several parameters that control the behaviour of the hashmap (optional)
// ############################################################################

struct my_hashmap_config {
    using size_type = DefaultHashmapConfig::size_type;
    using float_type = DefaultHashmapConfig::float_type;

    static constexpr size_type INIT_SIZE = 4;
    static constexpr size_type GROW_FACTOR = 2;
    static constexpr float_type MAX_LOAD_FACTOR = 0.75;
};

// ############################################################################
// Final hashmap configuration
// ############################################################################

using mapped_type = pm::persistent_ptr<value_type>;
using map_t = NVHashmap<
    my_string_hasher,
    mapped_type,
    my_hashmap_config
>;

// The root of the persistent memory object pool
struct root_t {
    pm::persistent_ptr<map_t> map;
};

using pool_t = pm::pool<root_t>;

// ############################################################################
// Some constants
// ############################################################################

const std::string poolLayout = "hashmap";
const size_t poolSize = 2 * PMEMOBJ_MIN_POOL;

// ############################################################################
// The test program
// ############################################################################

const std::string RESET = "\033[0m";
const std::string GREEN = "\033[0;32m";
const std::string RED = "\033[0;31m";

void usage()
{
    std::cout << "usage:\n";
    std::cout << "    hashTest FILE COMMAND\n\n";
    std::cout << "COMMAND:\n";
    std::cout << "    show [0 | 1]\n";
    std::cout << "        Print contents of the hash table. Flag = 1 includes empty buckets (default is 0).\n";
    std::cout << "    put KEY VALUE\n";
    std::cout << "        Inserts a value with the given key. Default value type is INT\n";
    std::cout << "    get KEY\n";
    std::cout << "        Retrieves the value associated with the given key if a matching pair exists.\n";
    std::cout << "    del KEY\n";
    std::cout << "        Removes the value associated with the given key if a matching pair exists\n";
    std::cout << "    delif VALUE\n";
    std::cout << "        Removes all pairs with the given value\n";
    std::cout << "    clear\n";
    std::cout << "        Removes all key-value pairs in this map. Number of buckets remains equal\n";
    std::cout << std::endl;
}

void launch(pool_t& pool, const std::string& cmd, const std::string& arg1,
        const std::string& arg2)
{
    auto root = pool.get_root();
    auto map = root->map;

    std::cout << "---before-----" << std::endl;
    std::cout << "buckets: " << map->buckets() << std::endl;
    std::cout << "elements: " << map->size() << std::endl;
    std::cout << "--------------\n" << std::endl;

    std::cout << "command: " << cmd << " " << arg1 << " " << arg2
            << std::endl;

    if (cmd == "show") {
        bool showEmptyBuckets = false;
        if (!arg1.empty())
            showEmptyBuckets = (std::stoi(arg1) == 1);
        map->show(showEmptyBuckets);
        // std::cout << "[\n";
        // size_t i = 0;
        // for (auto p : *map) {
        //     if (i != 0)
        //         std::cout << ",\n";
        //     std::cout << "  " <<  p->key.get_ro() << " -> " << p->value;
        //     ++i;
        // }
        // std::cout << "\n]" << std::endl;
        return;
    }

    std::cout << "status: ";
    if (cmd == "put" && !arg1.empty() && !arg2.empty()) {

        auto key = arg1;
        mapped_type value;

        pm::transaction::exec_tx(pool, [&](){
            value = pm::make_persistent<value_type>();
            *value = std::stoi(arg2);
        });

        auto status = map->put(key, value, pool);
        if (status)
            std::cout << GREEN << "success" << RESET << std::endl;
        else
            std::cout << RED << "failure" << RESET << std::endl;
    }
    else if (cmd == "get" && !arg1.empty()) {

        auto key = arg1;
        mapped_type value;
        auto status = map->get(key, value);
        if (status) {
            std::cout << GREEN << "success" << RESET << std::endl;
            std::cout << "result: " << *value << std::endl;
        }
        else
            std::cout << RED << "failure" << RESET << std::endl;
    }
    else if (cmd == "del" && !arg1.empty()) {

        auto key = arg1;
        auto status = map->erase(key, pool);
        if (status)
            std::cout << GREEN << "success" << RESET << std::endl;
        else
            std::cout << RED << "failure" << RESET << std::endl;
    }
    else if (cmd == "delif" && !arg1.empty()) {
        auto value_to_remove = std::stoi(arg1);
        size_t deleteCount = 0;
        auto end = map->end();
        for (auto it = map->begin(); it != end; ) {
            auto ptr = (*it)->value;
            if (*ptr == value_to_remove) {
                it = map->erase(it, pool);
                pm::transaction::exec_tx(pool, [&](){
                    pm::delete_persistent<int>(ptr);
                });
                ++deleteCount;
            }
            else {
                ++it;
            }
        }
        std::cout << "number of items removed: " << deleteCount << std::endl;
    }
    else if (cmd == "clear") {
        map->clear(pool);
    }
    else {
        std::cout << "error: invalid arguments" << std::endl;
        usage();
        return;
    }
    std::cout << "\n---after-----" << std::endl;
    std::cout << "buckets: " << map->buckets() << std::endl;
    std::cout << "elements: " << map->size() << std::endl;
    std::cout << "-------------" << std::endl;
}

}

int main(int argc, char* argv[])
{
    if (argc < 3) {
        std::cout << "error: too few arguments!\n";
        app::usage();
        return 0;
    }

    std::string file(argv[1]);
    std::string cmd(argv[2]);
    std::string arg1, arg2;
    if (argc > 3) arg1 = argv[3];
    if (argc > 4) arg2 = argv[4];

    app::pool_t pool;
    if (fs::exists(file)) {
        if (app::pool_t::check(file, app::poolLayout) != 1) {
            std::cout << "File seems to be corrupt! Aborting..." << std::endl;
            return 0;
        }
        std::cout << "File seems to be OK! Opening... ";
        pool = app::pool_t::open(file, app::poolLayout);
        std::cout << "OK\n";
    }
    else {
        std::cout << "File does not exist! Creating... "  << std::endl;
        pool = app::pool_t::create(file, app::poolLayout, app::poolSize);
        std::cout << "Root created! Initializing... " << std::endl;
        auto root = pool.get_root();
        pm::transaction::exec_tx(pool, [&](){
            root->map = pm::make_persistent<app::map_t>();
        });
        std::cout << "OK\n";
    }
    app::launch(pool, cmd, arg1, arg2);
    pool.close();
    return EXIT_SUCCESS;
}
