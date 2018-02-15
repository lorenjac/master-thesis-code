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
    midas::store store{pop};

    // Insert a value
    {
        auto tx = store.begin();
        store.write(tx, "sheep", "1");
        store.commit(tx);
    }

    std::cout << "\n*************************************\n\n";

    // Let one tx T1 update a version V but before T1 commits
    // another transaction T2 also updates V and commits.
    //
    // This is a write/write conflict (lost update).
    // This SI implementation employs the first-writer-wins policy
    // which is equivalent to the original first-committer-wins policy.
    // The first transaction that gets to update a data item gains
    // some sort of exclusive ownership (lock-free) on the item.
    // All contending transactions fail, making the first tx
    // the first to commit.
    //
    // T1 will atomically acquire the ownership of its data
    // and thus be able to update them. T2 however, is late
    // and will see that V is already being updated and fail.
    // A reader T3 proves that the version of the first
    // updater was applied.
    {
        store.print();

        std::cout << "\n*************************************\n\n";

        // T1
        auto updater1 = store.begin();
        store.write(updater1, "sheep", "2");

        store.print();

        std::cout << "\n*************************************\n\n";

        // T2
        auto updater2 = store.begin();
        store.write(updater2, "sheep", "3");
        store.commit(updater2);

        store.print();

        std::cout << "\n*************************************\n\n";

        // T1
        store.commit(updater1);

        store.print();

        // T3
        auto reader = store.begin();
        std::string result;
        store.read(reader, "sheep", result);
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
