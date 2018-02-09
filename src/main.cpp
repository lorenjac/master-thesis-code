#include <experimental/filesystem>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <string>
#include <tuple>

#include "store.hpp"

namespace fs = std::experimental::filesystem::v1;
namespace pm = pmem::obj;

namespace app {

using pool_type = midas::store::pool_type;
using command = std::tuple<std::string, std::string, std::string>;

const std::string RESET = "\033[0m";
const std::string GREEN = "\033[0;32m";
const std::string RED = "\033[0;31m";
const std::string CYAN = "\033[1;36m";

void usage()
{
    std::cout << "Commands:\n";
    std::cout << "  w KEY VALUE     Inserts or updates the specified pair\n";
    std::cout << "  r KEY           Retrieves the value associated with they key (if any)\n";
    std::cout << "  d KEY           Removes the pair with the given key (if any)\n";
    std::cout << "\nNote: This is an early alpha and still very buggy\n";
    std::cout << "\nKnown bugs:\n";
    std::cout << "  * after deleting a pair, it cannot be inserted again\n";
}

void execCommand(midas::store& store, const command& pack)
{
    const auto [cmd, key, value] = pack;
    if (cmd == "w" && key.size() && value.size()) {
        auto tx = store.begin();
        auto status = store.write(tx, key, value);
        if (status) {
            std::cout << RED << "write failed with status: ";
            std::cout << status << RESET << std::endl;
        }
        else {
            std::cout << GREEN << "write successful!" << RESET << std::endl;
        }

        status = store.commit(tx);
        if (status) {
            std::cout << RED << "commit failed with status: ";
            std::cout << status << RESET << std::endl;
        }
        else {
            std::cout << GREEN << "commit successful!" << RESET << std::endl;
        }
    }
    else if (cmd == "r" && key.size()) {
        auto tx = store.begin();
        std::string result;
        auto status = store.read(tx, key, result);
        if (status) {
            std::cout << RED << "read failed with status: ";
            std::cout << status << RESET << std::endl;
        }
        else {
            std::cout << GREEN << "read successful! -> " << RESET;
            std::cout << CYAN << result << RESET << std::endl;
        }

        status = store.commit(tx);
        if (status) {
            std::cout << RED << "commit failed with status: ";
            std::cout << status << RESET << std::endl;
        }
        else {
            std::cout << GREEN << "commit successful!" << RESET << std::endl;
        }
    }
    else if (cmd == "d" && key.size()) {
        auto tx = store.begin();
        auto status = store.drop(tx, key);
        if (status) {
            std::cout << RED << "drop failed with status: ";
            std::cout << status << RESET << std::endl;
        }
        else {
            std::cout << GREEN << "drop successful!" << RESET << std::endl;
        }

        status = store.commit(tx);
        if (status) {
            std::cout << RED << "commit failed with status: ";
            std::cout << status << RESET << std::endl;
        }
        else {
            std::cout << GREEN << "commit successful!" << RESET << std::endl;
        }
    }
    else {
        std::cout << "error: unknown command or missing arguments!\n";
        std::cout << "  cmd: " << cmd << '\n';
        std::cout << "  key: " << key << '\n';
        std::cout << "  val: " << value << '\n';
        usage();
    }
}

void launch(pool_type& pop, const command& pack)
{
    midas::store store{pop};
    if (std::get<0>(pack).empty()) {
        std::string input;
        std::string token;
        std::string cmd;
        std::string key;
        std::string val;
        for (;;) {
            std::cout << "Enter command (q for quit): ";
            std::string line;
            std::getline(std::cin, line);
            std::istringstream iss(line);
            std::vector<std::string> tokens{
                std::istream_iterator<std::string>{iss},
                std::istream_iterator<std::string>{}
            };
            for (unsigned i=0; i<tokens.size(); ++i) {
                if (i == 0)
                    cmd = tokens[0];
                else if (i == 1)
                    key = tokens[1];
                else if (i == 2)
                    val = tokens[2];
            }

            if (cmd == "q")
                break;
            else if (!cmd.empty())
                execCommand(store, std::make_tuple(cmd, key, val));

            std::cin.clear();

            // std::cout << "Press ENTER to proceed..." << std::endl;
            // std::cin.ignore();
        }
    }
    else {
        execCommand(store, pack);
    }
} // end function launch

} // end namespace app

int main(int argc, char* argv[])
{
    using pool_type = midas::store::pool_type;

    std::string file{"/tmp/nvm"};
    // std::string file{argv[1]};
    std::string cmd, arg1, arg2;
    if (argc > 1)
        cmd = argv[1];
    if (argc > 2)
        arg1 = argv[2];
    if (argc > 3)
        arg2 = argv[3];

    if (cmd == "h" || cmd == "-h" || cmd == "help" || cmd == "-help") {
        app::usage();
        return 0;
    }

    const std::string layout{"index"};
    const std::size_t pool_size = 64ULL * 1024 * 1024; // 64 MB
    pool_type pop;
    if (fs::exists(file)) {
        if (pool_type::check(file, layout) != 1) {
            std::cout << "File seems to be corrupt! Aborting..." << std::endl;
            return 0;
        }
        std::cout << "File seems to be OK!\n";
        std::cout << "Opening... ";
        pop = pool_type::open(file, layout);
        std::cout << "OK\n";
    }
    else {
        std::cout << "File does not exist! Creating... "  << std::endl;
        pop = pool_type::create(file, layout, pool_size);
        std::cout << "Root created! Initializing... " << std::endl;
        auto root = pop.get_root();
        pm::transaction::exec_tx(pop, [&](){
            root->index = pm::make_persistent<midas::store::index_type>();
        });
        std::cout << "OK\n";
    }
    app::launch(pop, std::make_tuple(cmd, arg1, arg2));
    pop.close();
    return EXIT_SUCCESS;
}
