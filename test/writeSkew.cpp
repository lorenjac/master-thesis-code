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
        store.write(tx, "sheep", "0");
        store.write(tx, "wolves", "0");
        store.commit(tx);
    }

    // C = we always want to have either wolves or sheep in the barn, not both

    std::cout << "\n*************************************\n\n";

    // Let one two transaction check some uniform condition and modify disjoint
    // data sets involved in this condition. No transaction commits before the
    // other has finished its operation.
    //
    // This some form of a read/write conflict (write skew). Normally they
    // should conflict but they succeed and the end result does not match the
    // predicate.
    //
    // The condition for both to apply their changes is that there are none of
    // the other kind (no wolves for sheep and vice versa). One controls the
    // number of sheep while the other controls the number of wolves.
    {
        // T1
        auto sheepUpdater = store.begin();
        std::string numSheep;
        std::string numWolves;
        store.read(sheepUpdater, "sheep", numSheep);
        store.read(sheepUpdater, "wolves", numWolves);
        if (std::stoi(numWolves) == 0) {
            store.write(
                sheepUpdater,
                "sheep",
                std::to_string(std::stoi(numSheep) + 1)
            );
        }

        // T2
        auto wolfUpdater = store.begin();
        store.read(wolfUpdater, "sheep", numSheep);
        store.read(wolfUpdater, "wolves", numWolves);
        if (std::stoi(numSheep) == 0) {
            store.write(
                wolfUpdater,
                "wolves",
                std::to_string(std::stoi(numWolves) + 1)
            );
        }

        // T1
        store.commit(sheepUpdater);

        // T2
        store.commit(wolfUpdater);

        // T3
        auto reader = store.begin();
        store.read(reader, "sheep", numSheep);
        store.read(reader, "wolves", numWolves);
        store.commit(reader);

        std::cout << "num sheep : " << numSheep << std::endl;
        std::cout << "num wolves: " << numWolves << std::endl;
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
