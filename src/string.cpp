#include "string.hpp"

namespace midas {
namespace detail {

std::ostream& operator<<(std::ostream& os, const NVString& str)
{
    const auto size = str.size.get_ro();
    os << "persistent_string [size=" << size;
    os << ", data={";
    for (NVString::size_type i=0; i<size; ++i)
        os << str.data[i];
    os << "}]";
    return os;
}

} // end namespace detail
} // end namespace midas
