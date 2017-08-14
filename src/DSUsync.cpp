#include <string>
#include <ndn-cxx/security/key-chain.hpp>
#include <ndn-cxx/face.hpp>
#include <ndn-cxx/util/scheduler.hpp>
#include <ndn-cxx/util/time.hpp>
//#include <ndn-group-encrypt/schedule.hpp>
//#include <mysql++/query.h>
#include <cstdio>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "helper.hpp"
#include "tcp_connection.hpp"
#include "schedule.hpp"
#include "rapidjson/document.h"		// rapidjson's DOM-style API
#include "rapidjson/prettywriter.h"	// for stringify JSON
#include "rapidjson/filestream.h"	// wrapper of C stream for prettywriter as output
/**
 * the logic of ndnfit-dsu:
 * 1. wait for register interest (there will be a register interest for each catalog generated by Android app)
 * 2. upon receiving register interest, go to fetch catalog data
 * 3. upon receiving catalog data, parse it, fetch data, c-key catalog (ckey catalog name is extracted from data name, check if c-key catalog is in the repo or not before fetching it, this check is very important, as it affects whether we need to fetch the following data serveral times), and cert (cert name is extracted from key locator, check if cert is in the repo or not before fetching it, the reason is similar to the previous one)
 * 4.1. upon receiving data, insert it into repo, done
 * 4.2. upon receiving ckey catalog, insert it into repo, parse it, fetch c-key, e-key and d-key catalog (e-key and d-key catalog names are extracted from c-key name, check if e-key and d-key catalog are in the repo or not before fetching the, the reason is similar to the previous one)
 * 4.3. upon receiving cert, insert it into repo, done
 * 5.1. upon receiving c-key, insert it into repo, done
 * 5.2. upon receiving e-key, insert it into repo, done
 * 5.3. upon receiving d-key catalog, insert into repo, parse it, and fetch d-key
 * 6. upon receiving d-key, insert it into repo, done
 * 7. upon receiving confirmation interest, check if the data is there in the repo, if it is, sends reply; if not, do nothing
 *
 * other complimentaty logic
 * there is a 2-layer map to keep track of all the interest, if an interest times out
 * 1. if it is data catalog interest, keep sending it, never stop
 * 2. if it is other interest, send at most 3 times, then remove the entry (TODO: revise the logic later, this logic has not been implemented)
 */
namespace ndn {
  namespace dsu {
    
    static const std::string COMMON_PREFIX = "/org/openmhealth";
    static const std::string UPDATE_INFO_SUFFIX = "/SAMPLE/fitness/physical_activity/time_location/update_info";
    static const std::string CATALOG_SUFFIX = "/SAMPLE/fitness/physical_activity/time_location/catalog";
    static const std::string DATA_SUFFIX = "/SAMPLE/fitness/physical_activity/time_location";
    
    static const std::string CATALOG = "catalog";
    static const std::string CKEY = "C-KEY";
    static const std::string EKEY = "E-KEY";
    static const std::string DKEY = "D-KEY";
    static const std::string DKEYCATALOG = "D-KEY/catalog";
    static const name::Component CATALOG_COMP(CATALOG);
    static const name::Component UPDATA_INFO_COMP("update_info");
    static const name::Component CKEY_COMP(CKEY);
    static const name::Component EKEY_COMP(EKEY);
    static const name::Component DKEY_COMP(DKEY);
    
    static const int INTEREST_TIME_OUT_SECONDS = 60;
    
    static const std::string CONFIRM_PREFIX = "/org/openmhealth/dsu/confirm/org/openmhealth";
    static const std::string REGISTER_PREFIX = "/org/openmhealth/dsu/register/org/openmhealth";
    static const std::string CONFIRM_PREFIX_FOR_REPLY = "/org/openmhealth/dsu/confirm";
    
    class DSUsync : noncopyable
    {
    public:
      DSUsync()
      : m_face(m_ioService) // Create face with io_service object
      , tcp_connection_for_putting_data(m_ioService, "localhost", "7376", bind(&DSUsync::putinDataCallback, this, _1))
      , tcp_connection_for_confirmation(m_ioService, "localhost", "7376", bind(&DSUsync::confirmationCallback, this, _1))
      , tcp_connection_for_local_check(m_ioService, "localhost", "7376",  bind(&DSUsync::localCheckCallback, this, _1))
      , m_scheduler(m_ioService)
      {
        tcp_connection_for_putting_data.startReceive();
        tcp_connection_for_confirmation.startReceive();
        tcp_connection_for_local_check.startReceive();
        fileToMap("state", user_unretrieve_map);
      }
      
      ~DSUsync() {
        saveStateToFile();
      }
      
      void saveStateToFile() {
        mapToFile("state", user_unretrieve_map);
        std::cout << "save state to file" << std::endl;
      }
      
      void
      run()
      {
        //accept incoming confirm interest
        m_face.setInterestFilter(CONFIRM_PREFIX,
                                 bind(&DSUsync::onConfirmInterest, this, _1, _2),
                                 RegisterPrefixSuccessCallback(),
                                 bind(&DSUsync::onRegisterFailed, this, _1, _2));
        
        //accept incoming register interest
        m_face.setInterestFilter(REGISTER_PREFIX,
                                 bind(&DSUsync::onRegisterInterest, this, _1, _2),
                                 RegisterPrefixSuccessCallback(),
                                 bind(&DSUsync::onRegisterFailed, this, _1, _2));
        
        // m_ioService.run() will block until all events finished or m_ioService.stop() is called
        m_ioService.run();
        
        // Alternatively, m_face.processEvents() can also be called.
        // processEvents will block until the requested data received or timeout occurs.
        // m_face.processEvents();
      }
      
    private:
      // when confirmation interest is received, check the repo first
      void confirmationCallback(const Block& wire) {
        std::cout << "confirmationCallback is called" << std::endl;
        if (wire.type() == ndn::tlv::Data) {
          Data data(wire);
          // if the data packet is there in the repo, send confirmation to the mobile device
          if(data.getContent().value_size() != 0) {
            shared_ptr<Data> confirmationData = make_shared<Data>();
            confirmationData->setName(Name(CONFIRM_PREFIX_FOR_REPLY).append(data.getName()));
            confirmationData->setFreshnessPeriod(time::seconds(10));
            
            // Sign Data packet with default identity
            m_keyChain.sign(*confirmationData);
            std::cout << "confirmationCallback sends out D: " << confirmationData->getName() << std::endl;
            m_face.put(*confirmationData);
          }
        }
        return;
      }
      
      /**
       * before fetch data, check
       * (1) whether the data is in the repo or not
       * (2) whether the interst has been sent or not
       */
      void localCheckCallback(const Block& wire) {
        if (wire.type() == ndn::tlv::Data) {
          Data data(wire);
          // if the data packet is not there in the repo, send interest to the mobile device to fetch data
          if (data.getContent().value_size() == 0) {
            name::Component user_id = data.getName().get(2);
            
            std::map<name::Component, std::map<Name, int>>::iterator outer_it;
            outer_it = user_unretrieve_map.find(user_id);
            std::map<Name, int>::iterator inner_it;
            if (outer_it != user_unretrieve_map.end()) {
              inner_it = outer_it->second.find(data.getName());
              if (inner_it != outer_it->second.end()) {
                // the ckey catalog/e-key/d-key catalog is hanlded before
                return;
              }
            }
            
            // 4 cases: ckeyCatalog, e-key, d-key catalog or cert
            Interest resentInterest(data.getName());
            resentInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
            resentInterest.setMustBeFresh(true);
            
            const Link * link = getLink(user_id);
            if(link != NULL) {
              resentInterest.setLink(link->wireEncode());
            }
            
            if (data.getName().toUri().find(CKEY) != std::string::npos) {
              // ckeycatalog
              m_face.expressInterest(resentInterest,
                                     bind(&DSUsync::onCKeyCatalog, this, _1, _2),
                                     bind(&DSUsync::onCkeyCatalogTimeout, this, _1));
            } else if(data.getName().toUri().find(EKEY) != std::string::npos) {
              //e-key
              m_face.expressInterest(resentInterest,
                                     bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                     bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
            } else if(data.getName().toUri().find(EKEY) != std::string::npos) {
              //d-key catalog
              m_face.expressInterest(resentInterest,
                                     bind(&DSUsync::onDKeyCatalog, this, _1, _2),
                                     bind(&DSUsync::onDkeyCatalogTimeout, this, _1));
            } else {
              // cert
              m_face.expressInterest(resentInterest,
                                     bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                     bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
            }
            
            std::cout << "localCheckCallback sends I: " << resentInterest.getName() << std::endl;
            outer_it->second[resentInterest.getName()] = 0;
          }
        }
        return;
      }
      
      // insert data into repo
      void putinDataCallback(const Block& wire) {
        if (wire.type() == ndn::tlv::Data) {
          Data data(wire);
          // if the data packet is not there in the repo, send interest to get data
          if(data.getContent().value_size() == 0) {
            Interest datapointInterest(data.getName());
            datapointInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
            datapointInterest.setMustBeFresh(true);
            
            // the component at position 2 is user_id
            const Link * link = getLink(data.getName().get(2));
            if(link != NULL) {
              datapointInterest.setLink(link->wireEncode());
            }
            
            m_face.expressInterest(datapointInterest,
                                   bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                   bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
            std::cout << "putinDataCallback sends I: " << datapointInterest << std::endl;
            std::map<name::Component, std::map<Name, int>>::iterator it;
            it = user_unretrieve_map.find(data.getName().get(2));
            if (it != user_unretrieve_map.end()) {
              it->second[datapointInterest.getName()] = 0;
            }
          }
        }
        return;
      }
      
      /**
       * when receive a data catalog, continue to fetch data and CKEY catalog
       * data calalog name example:
       * /org/openmhealth/haitao/SAMPLE/fitness/physical_activity/time_location/catalog/20170617T042400
       */
      void onCatalogData(const Interest& interest, const Data& data)
      {
        std::string content((char *)data.getContent().value(), data.getContent().value_size());
        std::cout << "onCatalogData receives D: " << data.getName() << std::endl;
        std::cout << content << std::endl;
//        char buffer[data.getContent().value_size()+1];
        char * buffer = new char[data.getContent().value_size()+1];
        std::strcpy(buffer, content.c_str());
        rapidjson::Document document;
        if (document.ParseInsitu<0>(buffer).HasParseError())
        {
          std::cout << "onCatalogData Parsing " << data.getName() << " error!" << std::endl;
        }
        
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it != outer_it->second.end()) {
            outer_it->second.erase(inner_it);
          }
        } else {
          //figure out what to do here
          return;
        }
        
        //put data into repo
        tcp_connection_for_putting_data.send(data.wireEncode());
        
        //parse the content and start to fetch the data points, see schema file for the details
        const rapidjson::Value& list = document;
        assert(list.IsArray());
        time::system_clock::TimePoint lastCatalogTimestamp = time::fromIsoString(data.getName().get(-1).toUri());
        //fetch c-key catalog
        if(list.Size() > 0) {
          time::system_clock::TimePoint ckeyHour = getRoundedTimeslot(lastCatalogTimestamp);
          Interest ckeyCatalogInterest(data.getName().getPrefix(-2).append("C-KEY").append("catalog").append(time::toIsoString(ckeyHour)));
          tcp_connection_for_local_check.send(ckeyCatalogInterest.wireEncode());
          /*
           ckeyCatalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
           ckeyCatalogInterest.setMustBeFresh(true);
           m_face.expressInterest(ckeyCatalogInterest,
           bind(&DSUsync::onCKeyCatalog, this, _1, _2),
           bind(&DSUsync::onCkeyCatalogTimeout, this, _1));*/
        }
        
        // fetch cert
        // assume the keylocator contains name, but this may change later
        Name certName = data.getSignature().getKeyLocator().getName();
        Interest certInterest(certName);
        tcp_connection_for_local_check.send(certInterest.wireEncode());
        
        //fetch data
        const Link * link = getLink(user_id);
        
        for (rapidjson::SizeType i = 0; i<list.Size(); i++) {
          assert(list[i].IsString());
          
          Interest datapointInterest(data.getName().getPrefix(-2).append(list[i].GetString()));
          datapointInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          datapointInterest.setMustBeFresh(true);
          
          if(link != NULL) {
            datapointInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(datapointInterest,
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
          std::cout << "onCatalogData sends I: " << datapointInterest << std::endl;
          outer_it->second[datapointInterest.getName()] = 0;
        }
        delete []buffer;
      }
      
      /**
       * when interest for data catalog times out, retry
       */
      void onCatalogTimeout (const Interest& interest)
      {
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it == outer_it->second.end()) {
            std::cout << "I didn't try to retrieve " << interest << std::endl;
            return;
          }
        } else {
          //figure out what to do here
          return;
        }
        
        int catalogRetry = inner_it->second;
        if(catalogRetry == INT_MAX) {
          std::cout << "onCatalogTimeout Timeout I: " << interest << std::endl;
          catalogRetry = 0;
        } else {
          Name previousName = interest.getName();
          Interest catalogInterest(interest.getName());
          catalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          catalogInterest.setMustBeFresh(true);
          
          const Link * link = getLink(user_id);
          if(link != NULL) {
            catalogInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(catalogInterest,
                                 bind(&DSUsync::onCatalogData, this, _1, _2),
                                 bind(&DSUsync::onCatalogTimeout, this, _1));
          std::cout << "onCatalogTimeout sends I: " << catalogInterest << std::endl;
          catalogRetry++;
        }
        inner_it->second = catalogRetry;
      }
      
      /**
       * upon receiving data, CKey, EKey, DKey or cert, insert into repo
       * data point name example:
       * /org/openmhealth/haitao/SAMPLE/fitness/physical_activity/time_location/20170617T042500/FOR/org/openmhealth/haitao/SAMPLE/fitness/physical_activity/time_location/C-KEY/20170617T040000
       * CKey name example:
       * /org/openmhealth/haitao/SAMPLE/fitness/physical_activity/time_location/C-KEY/20170617T040000/FOR/org/openmhealth/haitao/READ/fitness/E-KEY/20170617T000000/20170618T000000
       * EKey name example:
       * /org/openmhealth/haitao/READ/fitness/E-KEY/20170617T000000/20170618T000000
       * DKey name example:
       * /org/openmhealth/haitao/READ/fitness/D-KEY/20170617T000000/20170618T000000/FOR/<>
       * cert name example:
       * /org/openmhealth/haitao/KEY/ndnfit/ksk-1502442197258/ID-CERT
       */
      void onDatapointOrCKeyOrEKeyOrDKeyData(const Interest& interest, const Data& data)
      {
        std::string content((char *)data.getContent().value(), data.getContent().value_size());
        //        std::cout << content << std::endl;
        
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it != outer_it->second.end()) {
            outer_it->second.erase(inner_it);
          }
        } else {
          //figure out what to do here
          return;
        }
        
        //put data into repo
        tcp_connection_for_putting_data.send(data.wireEncode());
        
      }
      
      /**
       * when data or ckey interest times out, retry 3 times
       */
      void onDatapointOrCKeyOrEKeyOrDKeyTimeout (const Interest& interest)
      {
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it == outer_it->second.end()) {
            std::cout << "I didn't try to retrieve " << interest << std::endl;
            return;
          }
        } else {
          //figure out what to do here
          return;
        }
        
        int datapointRetry = inner_it->second;
        if(datapointRetry == 3) {
          std::cout << "onDatapointOrCKeyOrEKeyOrDKeyTimeout Timeout I: " << interest << std::endl;
          datapointRetry = 0;
        } else {
          Name previousName = interest.getName();
          Interest datapointInterest(interest.getName());
          datapointInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          datapointInterest.setMustBeFresh(true);
          
          const Link * link = getLink(user_id);
          if(link != NULL) {
            datapointInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(datapointInterest,
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
          std::cout << "onDatapointOrCKeyOrEKeyOrDKeyTimeout sending I: " << datapointInterest << std::endl;
          datapointRetry++;
        }
        inner_it->second = datapointRetry;
      }
      
      /**
       * upon receiving ckey catalog, continue to fetch
       * (1) ckey
       * (2) e-key
       * (3) d-key catalog
       * CKey catalog name example:
       * /org/openmhealth/haitao/SAMPLE/fitness/physical_activity/time_location/C-KEY/catalog/20170617T040000
       */
      void
      onCKeyCatalog (const Interest& interest, const Data& data)
      {
        std::string content((char *)data.getContent().value(), data.getContent().value_size());
        std::cout << "onCKeyCatalog receives D: " << data.getName() << std::endl;
        std::cout << content << std::endl;
//        char buffer[data.getContent().value_size()+1];
        char * buffer = new char[data.getContent().value_size()+1];
        std::strcpy(buffer, content.c_str());
        rapidjson::Document document;
        if (document.ParseInsitu<0>(buffer).HasParseError())
        {
          std::cout << "onCKeyCatalog Parsing " << data.getName() << " error!" << std::endl;
        }
        
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it != outer_it->second.end()) {
            outer_it->second.erase(inner_it);
          }
        } else {
          //figure out what to do here
          return;
        }
        
        //put data into repo
        tcp_connection_for_putting_data.send(data.wireEncode());
        
        //parse the content and start to fetch the data points, see schema file for the details
        const rapidjson::Value& list = document;
        assert(list.IsArray());
        for (rapidjson::SizeType i = 0; i<list.Size(); i++) {
          assert(list[i].IsString());
          //send out ckey insterest
          Interest ckeyInterest(Name(list[i].GetString()));
          ckeyInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          ckeyInterest.setMustBeFresh(true);
          
          const Link * link = getLink(user_id);
          if(link != NULL) {
            ckeyInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(ckeyInterest,
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
          std::cout << "onCKeyCatalog send I: " << ckeyInterest << std::endl;
          outer_it->second[ckeyInterest.getName()] = 0;
          //send out interest for E-key
          Interest ekeyInterest(ckeyInterest.getName().getSubName(interest.getName().size()));
          tcp_connection_for_local_check.send(ekeyInterest.wireEncode());
          //send out interest for d-key catalog
          std::string dkeyName(ekeyInterest.getName().toUri());
          replace(dkeyName, EKEY, DKEYCATALOG);
          Interest dkeyCatalogInterest(dkeyName);
          tcp_connection_for_local_check.send(dkeyCatalogInterest.wireEncode());
        }
        delete []buffer;
      }
      
      /**
       *
       */
      void onCkeyCatalogTimeout(const Interest& interest) {
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it == outer_it->second.end()) {
            std::cout << "I didn't try to retrieve " << interest << std::endl;
            return;
          }
        } else {
          //figure out what to do here
          return;
        }
        
        int ckeyCatalogRetry = inner_it->second;
        if(ckeyCatalogRetry == 3) {
          std::cout << "onCkeyCatalogTimeout Timeout I: " << interest << std::endl;
          ckeyCatalogRetry = 0;
        } else {
          Name previousName = interest.getName();
          Interest ckeyCatalogInterest(interest.getName());
          ckeyCatalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          ckeyCatalogInterest.setMustBeFresh(true);
          
          const Link * link = getLink(user_id);
          if(link != NULL) {
            ckeyCatalogInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(ckeyCatalogInterest,
                                 bind(&DSUsync::onCKeyCatalog, this, _1, _2),
                                 bind(&DSUsync::onCkeyCatalogTimeout, this, _1));
          std::cout << "onCkeyCatalogTimeout sending I: " << ckeyCatalogInterest << std::endl;
          ckeyCatalogRetry++;
        }
        inner_it->second = ckeyCatalogRetry;
      }
      
      /**
       * continue to retrieve DKey
       * DKey catalog name example:
       * /org/openmhealth/haitao/READ/fitness/D-KEY/catalog/20170617T000000/20170618T000000
       */
      void
      onDKeyCatalog (const Interest& interest, const Data& data)
      {
        std::string content((char *)data.getContent().value(), data.getContent().value_size());
        std::cout << "onDKeyCatalog receives D: " << data.getName() << std::endl;
        std::cout << content << std::endl;
//        char buffer[data.getContent().value_size()+1];
        char * buffer = new char[data.getContent().value_size()+1];
        std::strcpy(buffer, content.c_str());
        rapidjson::Document document;
        if (document.ParseInsitu<0>(buffer).HasParseError())
        {
          std::cout << "onDKeyCatalog Parsing " << data.getName() << " error!" << std::endl;
        }
        
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it != outer_it->second.end()) {
            outer_it->second.erase(inner_it);
          }
        } else {
          //figure out what to do here
          return;
        }
        
        //put data into repo
        tcp_connection_for_putting_data.send(data.wireEncode());
        
        //parse the content and start to fetch the data points, see schema file for the details
        const rapidjson::Value& list = document;
        assert(list.IsArray());
        for (rapidjson::SizeType i = 0; i<list.Size(); i++) {
          assert(list[i].IsString());
          //send out ckey insterest
          Interest dkeyInterest(Name(list[i].GetString()));
          dkeyInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          dkeyInterest.setMustBeFresh(true);
          
          const Link * link = getLink(user_id);
          if(link != NULL) {
            dkeyInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(dkeyInterest,
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyData, this, _1, _2),
                                 bind(&DSUsync::onDatapointOrCKeyOrEKeyOrDKeyTimeout, this, _1));
          std::cout << "onDKeyCatalog send I: " << dkeyInterest << std::endl;
          outer_it->second[dkeyInterest.getName()] = 0;
        }
        delete []buffer;
      }
      
      void
      onDkeyCatalogTimeout(const Interest& interest)
      {
        name::Component user_id = interest.getName().get(2);
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(interest.getName());
          if (inner_it == outer_it->second.end()) {
            std::cout << "I didn't try to retrieve " << interest << std::endl;
            return;
          }
        } else {
          //figure out what to do here
          return;
        }
        
        int dkeyCatalogRetry = inner_it->second;
        if(dkeyCatalogRetry == 3) {
          std::cout << "onDkeyCatalogTimeout Timeout I: " << interest << std::endl;
          dkeyCatalogRetry = 0;
        } else {
          Name previousName = interest.getName();
          Interest dkeyCatalogInterest(interest.getName());
          dkeyCatalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
          dkeyCatalogInterest.setMustBeFresh(true);
          
          const Link * link = getLink(user_id);
          if(link != NULL) {
            dkeyCatalogInterest.setLink(link->wireEncode());
          }
          
          m_face.expressInterest(dkeyCatalogInterest,
                                 bind(&DSUsync::onDKeyCatalog, this, _1, _2),
                                 bind(&DSUsync::onDkeyCatalogTimeout, this, _1));
          std::cout << "onDkeyCatalogTimeout sending I: " << dkeyCatalogInterest.getName() << std::endl;
          dkeyCatalogRetry++;
        }
        inner_it->second = dkeyCatalogRetry;
      }
      
      void
      onConfirmInterest(const InterestFilter& filter, const Interest& interest)
      {
        std::cout << "onConfirmInterest receives I: " << interest << std::endl;
        
        Interest dataInterest(interest.getName().getSubName(4));
        
        std::cout << "onConfirmInterest sends I to repo:" << dataInterest << std::endl;
        tcp_connection_for_confirmation.send(dataInterest.wireEncode());
      }
      
      /**
       * the android client sends register interest for each catalog
       * a name example:
       * /org/openmhealth/dsu/register/org/openmhealth/haitao/<timepoint>/<link object>
       * there may be no link object
       */
      void
      onRegisterInterest(const InterestFilter& filter, const Interest& interest)
      {
        std::cout << "onRegisterInterest receives I: " << interest << std::endl;
        Name registerSuccessDataName(interest.getName());
        name::Component user_id = registerSuccessDataName.get(6);
        
        std::map<name::Component, std::map<Name, int>>::iterator it;
        it = user_unretrieve_map.find(user_id);
        //send out catalog interest
        Name catalogName(COMMON_PREFIX);
        catalogName.append(user_id).append(Name(CATALOG_SUFFIX));
        name::Component timestamp = registerSuccessDataName.get(7);
        catalogName.append(timestamp);
        if(registerSuccessDataName.size() > 8) {
          Link link;
          link.wireDecode(interest.getName().get(-1).blockFromValue());
          std::cout << "onRegisterInterest got link: " << link << std::endl;
          user_link_map[user_id] = link;
        }
        Interest catalogInterest(catalogName);
        catalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
        catalogInterest.setMustBeFresh(true);
        
        const Link * link = getLink(user_id);
        if(link != NULL) {
          catalogInterest.setLink(link->wireEncode());
        }
        
        m_face.expressInterest(catalogInterest,
                               bind(&DSUsync::onCatalogData, this, _1, _2),
                               bind(&DSUsync::onCatalogTimeout, this, _1));
        std::cout << "onRegisterInterest sends I: " << catalogInterest << std::endl;
        
        std::map<Name, int> unretrieve_map;
        unretrieve_map[catalogInterest.getName()] = 0;
        user_unretrieve_map[user_id] = unretrieve_map;
        
        std::map<name::Component, std::map<Name, int>>::iterator outer_it;
        outer_it = user_unretrieve_map.find(user_id);
        std::map<Name, int>::iterator inner_it;
        if (outer_it != user_unretrieve_map.end()) {
          inner_it = outer_it->second.find(catalogInterest.getName());
          if (inner_it == outer_it->second.end()) {
            std::cout << "check if the interest is there " << catalogInterest << std::endl;
            return;
          }
        }
        
        shared_ptr<Data> data = make_shared<Data>();
        data->setName(registerSuccessDataName);
        data->setFreshnessPeriod(time::seconds(10));
        // Sign Data packet with default identity
        m_keyChain.sign(*data);
        std::cout << "onRegisterInterest sends D: " << data->getName() << std::endl;
        m_face.put(*data);
        
      }
      
      void
      onRegisterFailed(const Name& prefix, const std::string& reason)
      {
        std::cerr << "ERROR: Failed to register prefix \""
        << prefix << "\" in local hub's daemon (" << reason << ")"
        << std::endl;
      }
      
      const Link *
      getLink(const name::Component& user_id) const
      {
        std::map<name::Component, Link>::const_iterator link_iterator;
        link_iterator = user_link_map.find(user_id);
        if (link_iterator != user_link_map.end()) {
          return &(link_iterator->second);
        }
        return NULL;
      }
      
      
    private:
      // Explicitly create io_service object, which can be shared between Face and Scheduler
      boost::asio::io_service m_ioService;
      Face m_face;
      TcpConnection tcp_connection_for_putting_data;
      TcpConnection tcp_connection_for_confirmation;
      TcpConnection tcp_connection_for_local_check;
      Scheduler m_scheduler;
      std::map<name::Component, std::map<Name, int>> user_unretrieve_map;
      std::map<name::Component, Link> user_link_map;
      KeyChain m_keyChain;
    };
    
    
    
  } // namespace dsu
} // namespace ndn

ndn::dsu::DSUsync dsusync;

void my_handler(int s){
  printf("Caught signal %d\n",s);
  exit(1);
}

int
main(int argc, char** argv)
{
  struct sigaction sigIntHandler;
  
  sigIntHandler.sa_handler = my_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  
  sigaction(SIGINT, &sigIntHandler, NULL);
  
  try {
    dsusync.run();
  }
  catch (const std::exception& e) {
    std::cerr << "ERROR: " << e.what() << std::endl;
  }
  return 0;
}
