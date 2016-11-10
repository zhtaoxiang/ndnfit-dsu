#ifndef HELPER_HPP
#define HELPER_HPP

#include <ndn-cxx/util/time.hpp>
#include <map>
#include <string>
#include <ndn-cxx/name.hpp>

namespace ndn {
    namespace dsu {
        time::system_clock::TimePoint
        getRoundedTimeslot(const time::system_clock::TimePoint& timeslot);
        
        bool mapToFile(const std::string &, const std::map<name::Component,std::map<Name, int>> &);
        
        bool fileToMap(const std::string &, std::map<name::Component,std::map<Name, int>> &);
    }
}

#endif
