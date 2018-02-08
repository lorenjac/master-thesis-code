#include <experimental/filesystem>
#include <iostream>
#include <string>
#include <tuple>

#include "store.hpp"

namespace fs = std::experimental::filesystem::v1;
namespace pm = pmem::obj;

namespace app {

using pool_type = midas::store::pool_type;
using command = std::tuple<std::string, std::string, std::string>;

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

void launch(pool_type& pop, const command& pack)
{
    midas::store store{pop};

    auto [cmd, key, value] = pack;
    if (cmd.empty())
        return;

    if (cmd == "w" && key.size() && value.size()) {
        auto tx = store.begin();
        auto status = store.write(tx, key, value);
        if (status)
            std::cout << "write failed with status: " << status << std::endl;
        else
            std::cout << "write successful!" << std::endl;

        status = store.commit(tx);
        if (status)
            std::cout << "commit failed with status: " << status << std::endl;
        else
            std::cout << "commit successful!" << std::endl;
    }
    else if (cmd == "r" && key.size()) {
        auto tx = store.begin();
        std::string result;
        auto status = store.read(tx, key, result);
        if (status)
            std::cout << "read failed with status: " << status << std::endl;
        else
            std::cout << "result: " << result << std::endl;

        status = store.commit(tx);
        if (status)
            std::cout << "commit failed with status: " << status << std::endl;
        else
            std::cout << "commit successful!" << std::endl;
    }
    else if (cmd == "d" && key.size()) {
        auto tx = store.begin();
        auto status = store.drop(tx, key);
        if (status)
            std::cout << "drop failed with status: " << status << std::endl;
        else
            std::cout << "drop successful!" << std::endl;

        status = store.commit(tx);
        if (status)
            std::cout << "commit failed with status: " << status << std::endl;
        else
            std::cout << "commit successful!" << std::endl;
    }
    else {
        std::cout << "error: unknown command or missing arguments!\n";
        std::cout << "  cmd: " << cmd << '\n';
        std::cout << "  key: " << key << '\n';
        std::cout << "  val: " << value << '\n';
        usage();
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
