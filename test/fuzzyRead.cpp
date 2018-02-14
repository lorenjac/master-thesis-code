#include <experimental/filesystem>
#include <iostream>
#include <string>

#include "store.hpp"
#include "store_init.hpp"

namespace app {

// const std::string RESET = "\033[0m";
// const std::string GREEN = "\033[0;32m";
// const std::string RED = "\033[0;31m";
// const std::string CYAN = "\033[1;36m";

void launch(midas::pool_type& pop)
{
    midas::store store{pop};

    // Insert a value
    {
        auto tx = store.begin();
        store.write(tx, "sheep", "1");
        store.commit(tx);
    }

    std::cout << "\n*************************************\n\n";

    // Let one tx T2 update a version V that has just been
    // read by another tx T1 and will be read again.
    //
    // This is a read/write conflict (non-repeatable/inconistent/fuzzy read).
    // Since a transaction sees a snapshot of the database
    // as of its start time which is constant, a transaction
    // always sees the same data. Therefore no phantoms or
    // inconsistent reads are possible.
    //
    // T1 does not see the update to V and will simply read
    // the same item/value twice. Yet a later reader T3
    // proves that the value of T2 has been written.
    {
        // T1
        auto reader = store.begin();
        std::string result;
        store.read(reader, "sheep", result);
        std::cout << "T1: read -> " << result << std::endl;

        // T2
        auto updater = store.begin();
        store.write(updater, "sheep", "2");
        store.commit(updater);

        // T1
        result = "";
        store.read(reader, "sheep", result);
        std::cout << "T1: read -> " << result << std::endl;
        store.commit(reader);

        // T3
        auto laterReader = store.begin();
        result = "";
        store.read(laterReader, "sheep", result);
        std::cout << "T3: read -> " << result << std::endl;
        store.commit(laterReader);
    }

} // end function launch
} // end namespace app

int main(int argc, char* argv[])
{
    const std::string file{"/tmp/nvm"};
    const std::size_t size = 64ULL * 1024 * 1024; // 64 MB
    midas::pool_type pop;

    if (midas::init(pop, file, size)) {
        app::launch(pop);
        pop.close();
    }
    else {
        std::cout << "error: could not open file <" << file << ">!\n";
    }
    return EXIT_SUCCESS;
}
