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
