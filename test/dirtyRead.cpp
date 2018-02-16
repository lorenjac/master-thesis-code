#include <iostream>
#include <string>

#include "midas.hpp"

namespace app {

// const std::string RESET = "\033[0m";
// const std::string GREEN = "\033[0;32m";
// const std::string RED = "\033[0;31m";
// const std::string CYAN = "\033[1;36m";

void launch(midas::pop_type& pop)
{
    midas::Store store{pop};

    {
        auto tx = store.begin();
        store.write(tx, "sheep", "1");
        store.commit(tx);
    }

    std::cout << "\n*************************************n\n";

    // Let one tx T2 read a version V that has just been
    // updated by another tx T1.
    //
    // This is provokes a write/read conflict (uncommitted/dirty read).
    // By seeing a snapshot of the database, the reader only sees the
    // latest committed versions. Therefore, the update performed by
    // T1 is invisible to T2 and T2 simply reads the value that T1
    // read before it modified it.
    {
        // T1
        auto updater = store.begin();
        store.write(updater, "sheep", "2");

        // T2
        auto reader = store.begin();
        std::string result;
        store.read(reader, "sheep", result);
        std::cout << "T2: read -> " << result << std::endl;
        store.commit(reader);

        // t1
        store.commit(updater);
    }

} // end function launch

} // end namespace app

int main(int argc, char* argv[])
{
    const std::string file{"/tmp/nvm"};
    const std::size_t size = 64ULL * 1024 * 1024; // 64 MB
    midas::pop_type pop;

    if (midas::init(pop, file, size)) {
        app::launch(pop);
        pop.close();
    }
    else {
        std::cout << "error: could not open file <" << file << ">!\n";
    }
    return EXIT_SUCCESS;
}
