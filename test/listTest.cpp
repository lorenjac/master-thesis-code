#include <experimental/filesystem>
#include <iostream>
#include <string>
#include <tuple>

#include "list.hpp"

namespace fs = std::experimental::filesystem::v1;
namespace pm = pmem::obj;

namespace app {

    using midas::detail::list;

// CONSTANTS

const std::string poolLayout = "list";
const size_t poolSize = 2 * PMEMOBJ_MIN_POOL;

// TYPES

using elem_type = pm::p<int>;
using list_t = list<elem_type>;
struct root_t {
    pm::persistent_ptr<list_t> list;
};
using pool_t = pm::pool<root_t>;

using command_t = std::tuple<std::string, std::string, std::string>;

void usage()
{
    std::cout << "usage:\n";
    std::cout << "    listTest FILE COMMAND\n\n";
    std::cout << "COMMAND:\n";
    std::cout << "    show\n";
    std::cout << "        Print contents of the list.\n";
    std::cout << "    ab  VALUE\n";
    std::cout << "        Adds VALUE to the end of the list.\n";
    std::cout << "    af  VALUE\n";
    std::cout << "        Adds VALUE to the front of the list.\n";
    std::cout << "    i   POS VALUE\n";
    std::cout << "        Adds VALUE at POS in the list.\n";
    std::cout << "    g   INDEX\n";
    std::cout << "        Retrieves the value at the given position.\n";
    std::cout << "    d   INDEX\n";
    std::cout << "        Removes the value at the given position.\n";
    std::cout << "    dif VALUE\n";
    std::cout << "        Removes all elements the are equal to the given value.\n";
    std::cout << "    clear\n";
    std::cout << "        Removes all values from the list.\n";
    std::cout << "    double INDEX\n";
    std::cout << "        Doubles the value at the given position.\n";
    std::cout << "    move\n";
    std::cout << "        Moves list data to another (temporary) list.\n";
    std::cout << std::endl;
}

void launch(pool_t& pool, const command_t& command)
{
    auto root = pool.get_root();
    auto list = root->list;

    std::cout << "---before-----" << std::endl;
    std::cout << "elements: " << list->size() << std::endl;
    std::cout << "--------------\n" << std::endl;

    auto [cmd, arg1, arg2] = command;

    std::cout << "command: " << cmd << " " << arg1 << " " << arg2 << std::endl;
    if (cmd == "show") {
        std::cout << "[";
        size_t i = 0;
        for (const auto elem : *list) {
            if (i++ != 0)
                std::cout << ", ";

            std::cout << elem.get_ro();
        }
        std::cout << "]" << std::endl;
    }
    else if (cmd == "ab") {
        if (arg1.empty()) {
            usage();
            return;
        }
        list->push_back(std::stoi(arg1), pool);
    }
    else if (cmd == "af") {
        if (arg1.empty()) {
            usage();
            return;
        }
        list->push_front(std::stoi(arg1), pool);
    }
    else if (cmd == "i") {
        if (arg1.empty() || arg2.empty()) {
            usage();
            return;
        }
        list->insert(std::stoi(arg1), std::stoi(arg2), pool);
    }
    else if (cmd == "g") {
        if (arg1.empty()) {
            usage();
            return;
        }
        const auto result = list->get(std::stoi(arg1));
        std::cout << "result: " << result.get_ro() << std::endl;
    }
    else if (cmd == "d") {
        if (arg1.empty()) {
            usage();
            return;
        }
        auto index = std::stoi(arg1);
        list->erase(index, pool);
    }
    else if (cmd == "dif" && !arg1.empty()) {
        size_t deleteCount = 0;
        const auto to_be_removed = std::stoi(arg1);
        for (auto it = list->begin(); it != list->end(); ) {
            if (*it == to_be_removed) {
                it = list->erase(it, pool);
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
    else if (cmd == "double") {
        if (arg1.empty()) {
            usage();
            return;
        }
        const auto pos = std::stoi(arg1);
        pm::transaction::exec_tx(pool, [&](){
            auto& elem = list->get(pos);
            elem = 2 * elem;
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

                std::cout << (*it).get_ro();
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
    std::string arg1;
    std::string arg2;
    if (argc > 3)
        arg1 = argv[3];
    if (argc > 4)
        arg2 = argv[4];

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
    app::launch(pool, std::make_tuple(cmd, arg1, arg2));
    pool.close();
    return EXIT_SUCCESS;
}
