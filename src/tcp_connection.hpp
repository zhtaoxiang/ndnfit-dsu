#ifndef TCP_CONNECTION_HPP
#define TCP_CONNECTION_HPP

#include <cstddef>
#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <mutex>
#include <list>
#include <boost/asio.hpp>
#include <ndn-cxx/encoding/block.hpp>

namespace ndn {
  namespace dsu {
    
    const size_t MAX_NDN_PACKET_SIZE = 8800;
    
    class TcpConnection : boost::noncopyable
    {
    public:
      TcpConnection(boost::asio::io_service& ioService, const std::string& host, const std::string& port, const std::function<void(const Block& wire)> & receiveCallback)
      : m_socket(ioService)
      , m_hasStarted(false)
      , m_inputBufferSize(0)
      , m_receiveCallback(receiveCallback)
      {
        connect(ioService, host, port);
      }
      
      void
      startReceive()
      {
        BOOST_ASSERT(m_hasStarted);
        
        m_socket.async_receive(
                                        boost::asio::buffer(m_inputBuffer, MAX_NDN_PACKET_SIZE), 0,
                                        bind(&TcpConnection::handleReceive, this, std::placeholders::_1,
                                             std::placeholders::_2));
        
        m_hasStarted = true;
      }
      
      
      void
      onSuccessfullConnect(const boost::system::error_code& error);
      
      void send(const Block& block);
      
    private:
      void
      handleReceive(const boost::system::error_code& error,
                    std::size_t nBytesReceived);
      
      void
      connect(boost::asio::io_service& ioService, const std::string& host, const std::string& port);
      
      void
      asyncWrite();
      
      void
      handleAsyncWrite(const boost::system::error_code& error, std::list<Block>::iterator queueItem);
      
      
    private:
      boost::asio::ip::tcp::socket m_socket;
      bool m_hasStarted;
      uint8_t m_inputBuffer[MAX_NDN_PACKET_SIZE];
      std::size_t m_inputBufferSize;
      std::function<void(const Block& wire)> m_receiveCallback;
      std::list<Block> m_blockQueue;
    };
  }
}
#endif
