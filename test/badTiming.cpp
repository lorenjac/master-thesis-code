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

    // Insert a value
    {
        auto tx = store.begin();
        store.write(tx, "X", "1");
        store.commit(tx);
    }

    std::cout << "\n*************************************\n\n";

    {
        // T1
        auto updater1 = store.begin();
        store.write(updater1, "X", "2");

        // T2
        auto updater2 = store.begin();

        // T1
        store.commit(updater1);

        // T2
        store.write(updater2, "X", "3");
        store.commit(updater2);

        // T3
        auto reader = store.begin();
        std::string result;
        store.read(reader, "X", result);
        std::cout << "T3: read -> " << result << std::endl;
        store.commit(reader);
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
