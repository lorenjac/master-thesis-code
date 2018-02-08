#include "string.hpp"

namespace util {

std::ostream& operator<<(std::ostream& os, const string& str)
{
    const auto size = str.size.get_ro();
    os << "persistent_string [size=" << size;
    os << ", data={";
    for (string::size_type i=0; i<size; ++i)
        os << str.data[i];
    os << "}]";
    return os;
}

}
