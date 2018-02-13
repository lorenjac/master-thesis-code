#include <experimental/filesystem>
#include <iostream>
#include <string>

#include "list.hpp"

namespace fs = std::experimental::filesystem::v1;
namespace pm = pmem::obj;

namespace app {

// CONSTANTS

const std::string poolLayout = "list";
const size_t poolSize = 2 * PMEMOBJ_MIN_POOL;

// TYPES

// *** FOR POINTERS
using elem_type = pm::persistent_ptr<int>;

// *** FOR PRIMITIVES
// using elem_type = pm::p<int>;

using list_t = util::list<elem_type>;

struct root_t {
    pm::persistent_ptr<list_t> list;
};

using pool_t = pm::pool<root_t>;

void usage()
{
    std::cout << "usage:\n";
    std::cout << "    listTest FILE COMMAND\n\n";
    std::cout << "COMMAND:\n";
    std::cout << "    show\n";
    std::cout << "        Print contents of the vector.\n";
    std::cout << "    add VALUE\n";
    std::cout << "        Inserts an integer value into the vector.\n";
    std::cout << "    get INDEX\n";
    std::cout << "        Retrieves the value at the given position.\n";
    std::cout << "    del INDEX\n";
    std::cout << "        Removes the value at the given position.\n";
    std::cout << "    delif VALUE\n";
    std::cout << "        Removes all elements the are equal to the given value.\n";
    std::cout << "    clear\n";
    std::cout << "        Removes all values from the vector.\n";
    std::cout << "    double INDEX\n";
    std::cout << "        Doubles the value at the given position.\n";
    std::cout << "    move\n";
    std::cout << "        Moves list data to another (temporary) list.\n";
    std::cout << std::endl;
}

void launch(pool_t& pool, const std::string& cmd, const std::string& arg)
{
    auto root = pool.get_root();
    auto list = root->list;

    std::cout << "---before-----" << std::endl;
    std::cout << "elements: " << list->size() << std::endl;
    std::cout << "--------------\n" << std::endl;

    std::cout << "command: " << cmd << " " << arg << std::endl;
    if (cmd == "show") {
        std::cout << "[";
        size_t i = 0;
        for (const auto elem : *list) {
            if (i++ != 0)
                std::cout << ", ";

            // *** FOR POINTERS
            std::cout << *elem;

            // *** FOR PRIMITIVES
            // std::cout << elem.get_ro();
        }
        std::cout << "]" << std::endl;
    }
    else if (cmd == "add" && !arg.empty()) {

        // *** FOR POINTERS
        pm::transaction::exec_tx(pool, [&](){
            list->append(pm::make_persistent<int>(std::stoi(arg)), pool);
        });

        // *** FOR PRIMITIVES
        // list->append(std::stoi(arg), pool);
    }
    else if (cmd == "get" && !arg.empty()) {
        const auto result = list->get(std::stoi(arg));

        // *** FOR POINTERS
        std::cout << "result: " << *result << std::endl;

        // *** FOR PRIMITIVES
        // std::cout << "result: " << result.get_ro() << std::endl;
    }
    else if (cmd == "del" && !arg.empty()) {
        auto index = std::stoi(arg);
        auto ptr = list->get(index);
        list->erase(index, pool);
        pm::transaction::exec_tx(pool, [&](){
            pm::delete_persistent<int>(ptr);
        });
    }
    else if (cmd == "delif" && !arg.empty()) {
        size_t deleteCount = 0;
        const auto to_be_removed = std::stoi(arg);
        for (auto it = list->begin(); it != list->end(); ) {
            auto ptr = *it;
            if (*ptr == to_be_removed) {
                it = list->erase(it, pool);
                pm::transaction::exec_tx(pool, [&](){
                    pm::delete_persistent<int>(ptr);
                });
                ++deleteCount;
            }
            else
                ++it;
        }
        std::cout << "number of items removed: " << deleteCount << std::endl;
    }
    else if (cmd == "clear") {
        list->clear(pool);
    }
    else if (cmd == "double" && !arg.empty()) {
        const auto pos = std::stoi(arg);
        pm::transaction::exec_tx(pool, [&](){

            // *** FOR POINTERS
            auto elem_ptr = list->get(pos);
            *elem_ptr *= 2;

            // *** FOR PRIMITIVES
            // auto& elem = list->get(pos);
            // elem = 2 * elem;
        });
    }
    else if (cmd == "move") {
        pm::transaction::exec_tx(pool, [&](){
            auto ptr = pm::make_persistent<list_t>(std::move(*list));
            std::cout << "move-constructed list: [";
            std::cout << "size: " << ptr->size() << ", ";
            std::cout << "data: {";
            for (auto it = ptr->begin(); it != ptr->end(); ++it) {
                if (it != ptr->begin())
                    std::cout << ", ";

                // *** FOR POINTERS
                std::cout << *(*it);

                // *** FOR PRIMITIVES
                // std::cout << elem.get_ro();
            }
            std::cout << "}]" << std::endl;
            pm::delete_persistent<list_t>(ptr);
        });
    }
    else {
        std::cout << "error: invalid arguments" << std::endl;
        usage();
        return;
    }

    std::cout << "\n---after-----" << std::endl;
    std::cout << "elements: " << list->size() << std::endl;
    std::cout << "--------------" << std::endl;
}

} // end namespace app

int main(int argc, char* argv[])
{
    if (argc < 3) {
        std::cout << "error: too few arguments!\n";
        app::usage();
        return 0;
    }

    std::string file(argv[1]);
    std::string cmd(argv[2]);
    std::string arg;
    if (argc > 3)
        arg = argv[3];

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
            root->list = pm::make_persistent<app::list_t>();
        });

        std::cout << "OK\n";
    }
    app::launch(pool, cmd, arg);
    pool.close();
    return EXIT_SUCCESS;
}
