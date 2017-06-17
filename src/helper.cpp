#include "helper.hpp"
#include <fstream>
#include <vector>

namespace ndn {
  namespace dsu {
    typedef std::map<Name, int> nameCountMap;
    typedef std::map<name::Component,std::map<Name, int>> idNameMap;
    
    time::system_clock::TimePoint
    getRoundedTimeslot(const time::system_clock::TimePoint& timeslot) {
      return time::fromUnixTimestamp(
                                     (time::toUnixTimestamp(timeslot) / 3600000) * 3600000);
    }
    
    void splitString(std::vector<std::string> &v_str,const std::string &str,const char ch)
    {
      std::string sub;
      std::string::size_type pos = 0;
      std::string::size_type old_pos = 0;
      bool flag=true;
      
      while(flag)
      {
        pos=str.find_first_of(ch,pos);
        if(pos == std::string::npos)
        {
          flag = false;
          pos = str.size();
        }
        sub = str.substr(old_pos,pos-old_pos);  // Disregard the '.'
        v_str.push_back(sub);
        old_pos = ++pos;
      }
    }
    
    bool replace(std::string& str, const std::string& from, const std::string& to) {
      size_t start_pos = str.find(from);
      if(start_pos == std::string::npos)
        return false;
      str.replace(start_pos, from.length(), to);
      return true;
    }
    
    bool mapToFile(const std::string &filename,const idNameMap &fileMap)     //Write Map
    {
      std::ofstream ofile;
      ofile.open(filename.c_str());
      if(!ofile)
      {
        return false;           //file does not exist and cannot be created.
      }
      for(idNameMap::const_iterator iter = fileMap.begin(); iter!=fileMap.end(); ++iter)
      {
        std::cout << "write into file" << std::endl;
        ofile<<iter->first;
        for (nameCountMap::const_iterator iter2 = iter->second.begin(); iter2 != iter->second.end(); ++iter2) {
          ofile<<"\t"<<iter2->first;
        }
        ofile<<"\n";
      }
      return true;
    }
    
    bool fileToMap(const std::string &filename, idNameMap &fileMap)  //Read Map
    {
      std::ifstream ifile;
      ifile.open(filename.c_str());
      if(!ifile)
        return false;   //could not read the file.
      std::string line;
      name::Component id;
      std::vector<std::string> v_str;
      while(ifile>>line)
      {
        splitString(v_str,line,'\t');
        for(std::vector<std::string>::iterator iter = v_str.begin(); iter != v_str.end(); ++iter)        //First vector element is the key.
        {
          if(iter == v_str.begin())
          {
            id = name::Component(*iter);
            nameCountMap newMap;
            fileMap[id] = newMap;
            continue;
          }
          Name dataName(*iter);
          fileMap[id][dataName] = 0;
        }
      }
      return true;
    }
  }
}
