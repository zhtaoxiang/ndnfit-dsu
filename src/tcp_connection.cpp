#include "tcp_connection.hpp"
#include <ndn-cxx/data.hpp>

namespace ndn {
  namespace dsu {
    void
    TcpConnection::handleReceive(const boost::system::error_code& error,
                                 std::size_t nBytesReceived,
                                 const shared_ptr<TcpConnection>& client)
    {
      if (error)
      {
        if (error == boost::system::errc::operation_canceled) // when socket is closed by someone
          return;
        
        boost::system::error_code error;
        m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, error);
        m_socket.close(error);
        return;
      }
      
      m_inputBufferSize += nBytesReceived;
      
      // do magic
      
      std::size_t offset = 0;
      
      bool isOk = true;
      Block element;
      while (m_inputBufferSize - offset > 0) {
        std::tie(isOk, element) = Block::fromBuffer(m_inputBuffer + offset, m_inputBufferSize - offset);
        if (!isOk)
          break;
        
        offset += element.size();
        BOOST_ASSERT(offset <= m_inputBufferSize);
        
        if (element.type() == ndn::tlv::Data) {
          try {
            Data data(element);
            /*
             bool isInserted = m_writer.getStorageHandle().insertData(data);
             if (isInserted)
             std::cerr << "Successfully injected " << data.getName() << std::endl;
             else
             std::cerr << "FAILED to inject " << data.getName() << std::endl;
             */
          }
          catch (const std::runtime_error& error) {
            /// \todo Catch specific error after determining what wireDecode() can throw
            std::cerr << "Error decoding received Data packet" << std::endl;
          }
        }
      }
      
      if (!isOk && m_inputBufferSize == MAX_NDN_PACKET_SIZE && offset == 0)
      {
        boost::system::error_code error;
        m_socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, error);
        m_socket.close(error);
        return;
      }
      
      if (offset > 0)
      {
        if (offset != m_inputBufferSize)
        {
          std::copy(m_inputBuffer + offset, m_inputBuffer + m_inputBufferSize,
                    m_inputBuffer);
          m_inputBufferSize -= offset;
        }
        else
        {
          m_inputBufferSize = 0;
        }
      }
    
      m_socket.async_receive(boost::asio::buffer(m_inputBuffer + m_inputBufferSize,
                                                  MAX_NDN_PACKET_SIZE - m_inputBufferSize), 0,
                              bind(&TcpConnection::handleReceive, this, std::placeholders::_1,
                                   std::placeholders::_2, client));
    }
    
    void
    TcpConnection::connect(boost::asio::io_service& ioService, const std::string& host, const std::string& port)
    {
      using namespace boost::asio;
      
      ip::tcp::resolver resolver(ioService);
      ip::tcp::resolver::query query(host, port);
      
      ip::tcp::resolver::iterator endpoint = resolver.resolve(query);
      ip::tcp::resolver::iterator end;
      
      if (endpoint == end) {
        std::cerr << "Cannot resolve [" + host + ":" + port + "]" << std::endl;
        exit(1);
      }
      
      ip::tcp::endpoint serverEndpoint = *endpoint;
      
      m_socket.async_connect(serverEndpoint,
                           bind(&TcpConnection::onSuccessfullConnect, this, std::placeholders::_1));
    }
    
    void
    TcpConnection::onSuccessfullConnect(const boost::system::error_code& error)
    {
      if (error)
      {
        std::cerr << "TCP connection aborted" << std::endl;
        exit(1);
      }
    }
    
    void
    TcpConnection::send(Block& block)
    {
      m_blockQueue.push_back(block);
      if (m_blockQueue.size() == 1) {
        asyncWrite();
      }
    }
    
    void
    TcpConnection::asyncWrite() {
      BOOST_ASSERT(!m_blockQueue.empty());
      m_socket.async_send(boost::asio::buffer(m_blockQueue.front().wire(), m_blockQueue.front().size()),
                           bind(&TcpConnection::handleAsyncWrite, this, std::placeholders::_1, m_blockQueue.begin()));
    }
    
    void
    TcpConnection::handleAsyncWrite(const boost::system::error_code& error, std::list<Block>::iterator queueItem)
    {
      if (error) {
        if (error == boost::system::errc::operation_canceled) {
          // async receive has been explicitly cancelled (e.g., socket close)
          return;
        }
        exit(1);
      }
      
      m_blockQueue.erase(queueItem);
      if(!m_blockQueue.empty()) {
        asyncWrite();
      }
    }
  }
}
