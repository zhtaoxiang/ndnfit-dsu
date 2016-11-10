#include <string>
#include <ndn-cxx/security/key-chain.hpp>
#include <ndn-cxx/face.hpp>
#include <ndn-cxx/util/scheduler.hpp>
#include <ndn-cxx/util/time.hpp>
#include <ndn-group-encrypt/schedule.hpp>
//#include <mysql++/query.h>
#include <ndn-cxx/transport/tcp-transport.hpp>
#include <rapidjson/document.h>		// rapidjson's DOM-style API
#include <rapidjson/prettywriter.h>	// for stringify JSON
#include <rapidjson/filestream.h>	// wrapper of C stream for prettywriter as output
#include <cstdio>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "helper.hpp"

namespace ndn {
    namespace dsu {
        
        static const std::string COMMON_PREFIX = "/org/openmhealth";
        static const std::string UPDATE_INFO_SUFFIX = "/SAMPLE/fitness/physical_activity/time_location/update_info";
        static const std::string CATALOG_SUFFIX = "/SAMPLE/fitness/physical_activity/time_location/catalog";
        static const std::string DATA_SUFFIX = "/SAMPLE/fitness/physical_activity/time_location";
        
        static const name::Component CATALOG_COMP("catalog");
        static const name::Component UPDATA_INFO_COMP("update_info");
        static const name::Component CKEY_COMP("C-KEY");
        
        static const int INTEREST_TIME_OUT_SECONDS = 60;
        
        static const std::string CONFIRM_PREFIX = "/ndn/edu/ucla/remap/ndnfit/dsu/confirm/org/openmhealth";
        static const std::string REGISTER_PREFIX = "/ndn/edu/ucla/remap/ndnfit/dsu/register/org/openmhealth";
        static const std::string CONFIRM_PREFIX_FOR_REPLY = "/ndn/edu/ucla/remap/ndnfit/dsu/confirm";
        
        class DSUsync : noncopyable
        {
        public:
            DSUsync()
            : m_face(m_ioService) // Create face with io_service object
            , tcp_connect_repo_for_put_data("localhost", "7376")
            , tcp_connect_repo_for_confirmation("localhost", "7376")
            , tcp_connect_repo_for_local_check("localhost", "7376")
            , m_scheduler(m_ioService)
            {
                tcp_connect_repo_for_put_data.connect(m_ioService, bind(&DSUsync::putinDataCallback, this, _1));
                tcp_connect_repo_for_confirmation.connect(m_ioService, bind(&DSUsync::confirmationCallback, this, _1));
                tcp_connect_repo_for_local_check.connect(m_ioService, bind(&DSUsync::localCheckCallback, this, _1));
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
            void confirmationCallback(const Block& wire) {
                if (wire.type() == ndn::tlv::Data) {
                    Data data(wire);
                    // if the data packet is there in the repo, send confirmation to the mobile device
                    if(data.getContent().value_size() != 0) {
                        shared_ptr<Data> confirmationData = make_shared<Data>();
                        confirmationData->setName(Name(CONFIRM_PREFIX_FOR_REPLY).append(data.getName()));
                        confirmationData->setFreshnessPeriod(time::seconds(10));
                        
                        // Sign Data packet with default identity
                        m_keyChain.sign(*confirmationData);
                        std::cout << "confirmationCallback sends out D: " << *confirmationData << std::endl;
                        m_face.put(*confirmationData);
                    }
                }
                return;
            }
            
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
                                // the ckey catalog is hanlded before
                                return;
                            }
                        }
                        
                        Interest ckeyCatalogInterest(data.getName());
                        ckeyCatalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                        ckeyCatalogInterest.setMustBeFresh(true);
                        m_face.expressInterest(ckeyCatalogInterest,
                                               bind(&DSUsync::onCKeyCatalog, this, _1, _2),
                                               bind(&DSUsync::onCkeyCatalogTimeout, this, _1));
                        std::cout << "localCheckCallback sends I: " << ckeyCatalogInterest << std::endl;
                        outer_it->second[ckeyCatalogInterest.getName()] = 0;
                    }
                }
                return;
            }
            
            void putinDataCallback(const Block& wire) {
                if (wire.type() == ndn::tlv::Data) {
                    Data data(wire);
                    // if the data packet is not there in the repo, send interest to get data
                    if(data.getContent().value_size() == 0) {
                        Interest datapointInterest(data.getName());
                        datapointInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                        datapointInterest.setMustBeFresh(true);
                        m_face.expressInterest(datapointInterest,
                                               bind(&DSUsync::onDatapointOrCKeyData, this, _1, _2),
                                               bind(&DSUsync::onDatapointOrCKeyTimeout, this, _1));
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
            /*
            void onUpdateInfoData(const Interest& interest, const Data& data)
            {
                std::string content((char *)data.getContent().value(), data.getContent().value_size());
                std::cout << content << std::endl;
                char buffer[data.getContent().value_size()+1];
                std::strcpy(buffer, content.c_str());
                rapidjson::Document document;
                if (document.ParseInsitu<0>(buffer).HasParseError())
                {
                    std::cout << "Parsing " << data << " error!" << std::endl;
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
                
                // add into confirm set
//                std::map<name::Component, std::set<Name>>::iterator it_confirm;
//                it_confirm = user_confirm_map.find(user_id);
//                if (it_confirm != user_confirm_map.end()) {
//                    it_confirm->second.insert(interest.getName());
//                }
                //put data into repo
                tcp_connect_repo_for_put_data.send(data.wireEncode());
                
                //parse the content and start to fetch catalog packets, see schema file for the details
                const rapidjson::Value& list = document;
                assert(list.IsArray());
                for (rapidjson::SizeType i = 0; i<list.Size(); i++) {
                    assert(list[i].IsObject());
                    assert(list[i]["timepoint"].IsNumber());
                    assert(list[i]["timepoint"].IsUInt64());
                    assert(list[i]["version"].IsNumber());
                    assert(list[i]["version"].IsUInt64());
                    
                    //send out catalog interest
                    Interest catalogInterest(interest.getName().getPrefix(-2).append("catalog")
                                             .appendTimestamp(time::fromUnixTimestamp(time::milliseconds(list[i]["timepoint"].GetUint64() / 1000)))
                                             .appendVersion(list[i]["version"].GetUint64()));
                    catalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                    catalogInterest.setMustBeFresh(true);
                    m_face.expressInterest(catalogInterest,
                                           bind(&DSUsync::onCatalogData, this, _1, _2),
                                           bind(&DSUsync::onCatalogTimeout, this, _1));
                    std::cout << "Sending " << catalogInterest << std::endl;
                    //                    unretrieveMap.insert(std::pair<Name, int>(catalogInterest.getName(), 0));
                    outer_it->second[catalogInterest.getName()] = 0;
                }
                
                // continue to fetch the next update information packet
                uint64_t seqNo = interest.getName().get(-1).toSequenceNumber();
                Interest updateInfoInterest(interest.getName().getPrefix(-1).appendSequenceNumber(seqNo + 1));
                updateInfoInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                updateInfoInterest.setMustBeFresh(true);
                m_face.expressInterest(updateInfoInterest,
                                       bind(&DSUsync::onUpdateInfoData, this, _1, _2),
                                       bind(&DSUsync::onUpdateInfoTimeout, this, _1));
                std::cout << "Sending " << updateInfoInterest << std::endl;
                //                unretrieveMap.insert(std::pair<Name, int>(updateInfoInterest.getName(), 0));
                outer_it->second[updateInfoInterest.getName()] = 0;
            }
            
            void onUpdateInfoTimeout (const Interest& interest)
            {
                name::Component user_id = interest.getName().get(2);
                
                std::map<name::Component, std::map<Name, int>>::iterator outer_it;
                outer_it = user_unretrieve_map.find(user_id);
                std::map<Name, int>::iterator inner_it;
                if (outer_it != user_unretrieve_map.end()) {
                    inner_it = outer_it->second.find(interest.getName());
                    if (inner_it == outer_it->second.end()) {
                        std::cout << "I didn't try to retrieve " << interest.getName().toUri()<< std::endl;
                        return;
                    }
                } else {
                    //figure out what to do here
                    return;
                }
                
                
                int updateInfoRetry = inner_it->second;
                if(updateInfoRetry == INT_MAX) {
                    std::cout << "Timeout " << interest << std::endl;
                    updateInfoRetry = 0;
                } else {
                    Interest updateInfoInterest(interest.getName());
                    updateInfoInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                    updateInfoInterest.setMustBeFresh(true);
                    m_face.expressInterest(updateInfoInterest,
                                           bind(&DSUsync::onUpdateInfoData, this, _1, _2),
                                           bind(&DSUsync::onUpdateInfoTimeout, this, _1));
                    std::cout << "Sending " << updateInfoInterest << std::endl;
                    updateInfoRetry++;
                }
                inner_it->second = updateInfoRetry;
            }*/
            
            void onCatalogData(const Interest& interest, const Data& data)
            {
                std::string content((char *)data.getContent().value(), data.getContent().value_size());
                std::cout << "onCatalogData receives D: " << data << std::endl;
                std::cout << content << std::endl;
                char buffer[data.getContent().value_size()+1];
                std::strcpy(buffer, content.c_str());
                rapidjson::Document document;
                if (document.ParseInsitu<0>(buffer).HasParseError())
                {
                    std::cout << "onCatalogData Parsing " << data << " error!" << std::endl;
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
                
                // add into confirm set
//                std::map<name::Component, std::set<Name>>::iterator it_confirm;
//                it_confirm = user_confirm_map.find(user_id);
//                if (it_confirm != user_confirm_map.end()) {
//                    it_confirm->second.insert(interest.getName());
//                }
                
                //put data into repo
                tcp_connect_repo_for_put_data.send(data.wireEncode());
                
                //parse the content and start to fetch the data points, see schema file for the details
                const rapidjson::Value& list = document;
                assert(list.IsArray());
                time::system_clock::TimePoint lastCatalogTimestamp = time::fromIsoString(data.getName().get(-1).toUri());
                //fetch c-key catalog
                if(list.Size() > 0) {
                    time::system_clock::TimePoint ckeyHour = getRoundedTimeslot(lastCatalogTimestamp);
                    Interest ckeyCatalogInterest(data.getName().getPrefix(-2).append("C-KEY").append("catalog").append(time::toIsoString(ckeyHour)));
                    tcp_connect_repo_for_local_check.send(ckeyCatalogInterest.wireEncode());
                }
                for (rapidjson::SizeType i = 0; i<list.Size(); i++) {
                    //assert(list[i].IsNumber());
                    //assert(list[i].IsUInt64());
                    assert(list[i].IsString());
                    
                    //std::cout << list[i].GetUint64() << std::endl;
                    // send out datapoints interest
                    //Interest datapointInterest(data.getName().getPrefix(-2).appendTimestamp(time::fromUnixTimestamp(time::milliseconds(list[i].GetUint64()/1000))));
                    Interest datapointInterest(data.getName().getPrefix(-2).append(list[i].GetString()));
                    datapointInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                    datapointInterest.setMustBeFresh(true);
                    
                    //tcp_connect_repo_for_put_data.send(datapointInterest.wireEncode());
                    m_face.expressInterest(datapointInterest,
                                                        bind(&DSUsync::onDatapointOrCKeyData, this, _1, _2),
                                                        bind(&DSUsync::onDatapointOrCKeyTimeout, this, _1));
                    std::cout << "onCatalogData sends I: " << datapointInterest << std::endl;
                    outer_it->second[datapointInterest.getName()] = 0;
                }
                
                // continue to fetch the next catalog packet
                
                //time::system_clock::TimePoint lastCatalogTimestamp = time::fromIsoString(data.getName().get(-1).toUri());
                Interest catalogInterest(data.getName().getPrefix(-1).append(time::toIsoString(lastCatalogTimestamp + time::minutes(2))));
                catalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                catalogInterest.setMustBeFresh(true);
                m_face.expressInterest(catalogInterest,
                                       bind(&DSUsync::onCatalogData, this, _1, _2),
                                       bind(&DSUsync::onCatalogTimeout, this, _1));
                std::cout << "onCatalogData sends I: " << catalogInterest << std::endl;
                //                unretrieveMap.insert(std::pair<Name, int>(updateInfoInterest.getName(), 0));
                outer_it->second[catalogInterest.getName()] = 0;
                
                
            }
            
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
                    m_face.expressInterest(catalogInterest,
                                           bind(&DSUsync::onCatalogData, this, _1, _2),
                                           bind(&DSUsync::onCatalogTimeout, this, _1));
                    std::cout << "onCatalogTimeout sends I: " << catalogInterest << std::endl;
                    catalogRetry++;
                }
                inner_it->second = catalogRetry;
            }
            
            void onDatapointOrCKeyData(const Interest& interest, const Data& data)
            {
                std::string content((char *)data.getContent().value(), data.getContent().value_size());
                std::cout << content << std::endl;
                // to correctly parse the JSON, the '\0' should be included.
                //char buffer[data.getContent().value_size()+1];
                //std::strcpy(buffer, content.c_str());
                //rapidjson::Document document;
                
                //if (document.ParseInsitu<0>(buffer).HasParseError())
                //{
                //    std::cout << "onDatapointData Parsing " << data << " error!" << std::endl;
                //}
                
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
                
                // add into confirm set
//                std::map<name::Component, std::set<Name>>::iterator it_confirm;
//                it_confirm = user_confirm_map.find(user_id);
//                if (it_confirm != user_confirm_map.end()) {
//                    it_confirm->second.insert(interest.getName());
//                }
                
                //put data into repo
                tcp_connect_repo_for_put_data.send(data.wireEncode());
                
            }
            
            void onDatapointOrCKeyTimeout (const Interest& interest)
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
                    std::cout << "onDatapointTimeout Timeout I: " << interest << std::endl;
                    datapointRetry = 0;
                } else {
                    Name previousName = interest.getName();
                    Interest datapointInterest(interest.getName());
                    datapointInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                    datapointInterest.setMustBeFresh(true);
                    m_face.expressInterest(datapointInterest,
                                           bind(&DSUsync::onDatapointOrCKeyData, this, _1, _2),
                                           bind(&DSUsync::onDatapointOrCKeyTimeout, this, _1));
                    std::cout << "onDatapointTimeout sending I: " << datapointInterest << std::endl;
                    datapointRetry++;
                }
                inner_it->second = datapointRetry;
            }
            
            void onCKeyCatalog (const Interest& interest, const Data& data)
            {
                std::string content((char *)data.getContent().value(), data.getContent().value_size());
                std::cout << "onCKeyCatalog receives D: " << data << std::endl;
                std::cout << content << std::endl;
                char buffer[data.getContent().value_size()+1];
                std::strcpy(buffer, content.c_str());
                rapidjson::Document document;
                if (document.ParseInsitu<0>(buffer).HasParseError())
                {
                    std::cout << "onCKeyCatalog Parsing " << data << " error!" << std::endl;
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
                tcp_connect_repo_for_put_data.send(data.wireEncode());
            
                //parse the content and start to fetch the data points, see schema file for the details
                const rapidjson::Value& list = document;
                assert(list.IsArray());
                for (rapidjson::SizeType i = 0; i<list.Size(); i++) {
                    assert(list[i].IsString());
                    //send out ckey insterest
                    Interest ckeyInterest(Name(list[i].GetString()));
                    ckeyInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                    ckeyInterest.setMustBeFresh(true);
                
                    //tcp_connect_repo_for_put_data.send(datapointInterest.wireEncode());
                    m_face.expressInterest(ckeyInterest,
                                       bind(&DSUsync::onDatapointOrCKeyData, this, _1, _2),
                                       bind(&DSUsync::onDatapointOrCKeyTimeout, this, _1));
                    std::cout << "onCKeyCatalog send I: " << ckeyInterest << std::endl;
                    outer_it->second[ckeyInterest.getName()] = 0;
                }
            }
            
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
                    m_face.expressInterest(ckeyCatalogInterest,
                                           bind(&DSUsync::onCKeyCatalog, this, _1, _2),
                                           bind(&DSUsync::onCkeyCatalogTimeout, this, _1));
                    std::cout << "onCkeyCatalogTimeout sending I: " << ckeyCatalogInterest << std::endl;
                    ckeyCatalogRetry++;
                }
                inner_it->second = ckeyCatalogRetry;
            }
            
            void
            onConfirmInterest(const InterestFilter& filter, const Interest& interest)
            {
                std::cout << "onConfirmInterest receives I: " << interest << std::endl;
                
                Interest dataInterest(interest.getName().getSubName(7));
                
                tcp_connect_repo_for_confirmation.send(dataInterest.wireEncode());
                
                // Create new name, based on Interest's name
//                Name confirmDataName(interest.getName());
//                name::Component user_id = confirmDataName.get(9);
//                
//                std::map<name::Component, std::set<Name>>::iterator outer_it;
//                outer_it = user_confirm_map.find(user_id);
//                std::set<Name>::iterator inner_it;
//                if (outer_it != user_confirm_map.end()) {
//                    inner_it = outer_it->second.find(confirmDataName.getSubName(7));
//                    if (inner_it != outer_it->second.end()) {
//                        shared_ptr<Data> data = make_shared<Data>();
//                        data->setName(confirmDataName);
//                        data->setFreshnessPeriod(time::seconds(10));
//                        
//                        // Sign Data packet with default identity
//                        m_keyChain.sign(*data);
//                        std::cout << ">> D: " << *data << std::endl;
//                        m_face.put(*data);
//                        
//                        outer_it->second.erase(inner_it);
//                    }
//                } else {
//                    std::cout<< "I don't know the user " << user_id << std::endl;
//                }
            }
            
            void
            onRegisterInterest(const InterestFilter& filter, const Interest& interest)
            {
                std::cout << "onRegisterInterest receives I: " << interest << std::endl;
                Name registerSuccessDataName(interest.getName());
                name::Component user_id = registerSuccessDataName.get(9);
                
                std::map<name::Component, std::map<Name, int>>::iterator it;
                it = user_unretrieve_map.find(user_id);
                if (it != user_unretrieve_map.end()) {
                    for (std::map<Name,int>::iterator inner_it=it->second.begin(); inner_it!=it->second.end(); ++inner_it) {
                        name::Component keyComp = inner_it->first.get(7);
                        Interest resentInterest(inner_it->first);
                        resentInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                        resentInterest.setMustBeFresh(true);
                        // since the same update information interest keeps being sent, so we do not need to do the following part
//                        if (keyComp == UPDATA_INFO_COMP) {
//                            m_face.expressInterest(resentInterest,
//                                                   bind(&DSUsync::onUpdateInfoData, this, _1, _2),
//                                                   bind(&DSUsync::onUpdateInfoTimeout, this, _1));
//                            
//                        } else
                        if (keyComp == CKEY_COMP) {
                            name::Component key2Comp = inner_it->first.get(8);
                            if (key2Comp == CATALOG_COMP) {
                                m_face.expressInterest(resentInterest,
                                                       bind(&DSUsync::onCKeyCatalog, this, _1, _2),
                                                       bind(&DSUsync::onCkeyCatalogTimeout, this, _1));
                            }
                            else {
                                m_face.expressInterest(resentInterest,
                                                       bind(&DSUsync::onDatapointOrCKeyData, this, _1, _2),
                                                       bind(&DSUsync::onDatapointOrCKeyTimeout, this, _1));
                            }
                        }
                        else if(keyComp != CATALOG_COMP) {
                            m_face.expressInterest(resentInterest,
                                                   bind(&DSUsync::onDatapointOrCKeyData, this, _1, _2),
                                                   bind(&DSUsync::onDatapointOrCKeyTimeout, this, _1));
                        }
                        std::cout << "onRegisterInterest sends I: " << resentInterest << std::endl;
                    }
                } else {
                    //send out catalog interest
                    Interest catalogInterest(Name(COMMON_PREFIX).append(user_id).append(Name(CATALOG_SUFFIX)));
                    catalogInterest.setInterestLifetime(time::seconds(INTEREST_TIME_OUT_SECONDS));
                    catalogInterest.setMustBeFresh(true);
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
                    
//                    std::set<Name> confirm_set;
//                    user_confirm_map[user_id] = confirm_set;
                }
                
                shared_ptr<Data> data = make_shared<Data>();
                data->setName(registerSuccessDataName);
                data->setFreshnessPeriod(time::seconds(10));
                // Sign Data packet with default identity
                m_keyChain.sign(*data);
                std::cout << "onRegisterInterest sends D: " << *data << std::endl;
                m_face.put(*data);
                
            }
            
            void
            onRegisterFailed(const Name& prefix, const std::string& reason)
            {
                std::cerr << "ERROR: Failed to register prefix \""
                << prefix << "\" in local hub's daemon (" << reason << ")"
                << std::endl;
            }
            
        private:
            // Explicitly create io_service object, which can be shared between Face and Scheduler
            boost::asio::io_service m_ioService;
            Face m_face;
            TcpTransport tcp_connect_repo_for_put_data;
            TcpTransport tcp_connect_repo_for_confirmation;
            TcpTransport tcp_connect_repo_for_local_check;
            Scheduler m_scheduler;
            std::map<name::Component, std::map<Name, int>> user_unretrieve_map;
//            std::map<name::Component, std::set<Name>> user_confirm_map;
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
    
    /* this is a test to see if "save state to file" works or not
    std::map<ndn::name::Component, std::map<ndn::Name, int>> user_unretrieve_map;
    ndn::dsu::fileToMap("a", user_unretrieve_map);
    ndn::dsu::mapToFile("b", user_unretrieve_map);
    */
    
    try {
        dsusync.run();
    }
    catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
    }
    return 0;
}
