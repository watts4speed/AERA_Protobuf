//_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
//_/_/
//_/_/ AERA
//_/_/ Autocatalytic Endogenous Reflective Architecture
//_/_/ 
//_/_/ Copyright (c) 2018-2026 Jeff Thompson
//_/_/ Copyright (c) 2018-2026 Kristinn R. Thorisson
//_/_/ Copyright (c) 2018-2026 Icelandic Institute for Intelligent Machines
//_/_/ Copyright (c) 2021-2026 Leonard Eberding
//_/_/ http://www.iiim.is
//_/_/
//_/_/ --- Open-Source BSD License, with CADIA Clause v 1.0 ---
//_/_/
//_/_/ Redistribution and use in source and binary forms, with or without
//_/_/ modification, is permitted provided that the following conditions
//_/_/ are met:
//_/_/ - Redistributions of source code must retain the above copyright
//_/_/   and collaboration notice, this list of conditions and the
//_/_/   following disclaimer.
//_/_/ - Redistributions in binary form must reproduce the above copyright
//_/_/   notice, this list of conditions and the following disclaimer 
//_/_/   in the documentation and/or other materials provided with 
//_/_/   the distribution.
//_/_/
//_/_/ - Neither the name of its copyright holders nor the names of its
//_/_/   contributors may be used to endorse or promote products
//_/_/   derived from this software without specific prior 
//_/_/   written permission.
//_/_/   
//_/_/ - CADIA Clause: The license granted in and to the software 
//_/_/   under this agreement is a limited-use license. 
//_/_/   The software may not be used in furtherance of:
//_/_/    (i)   intentionally causing bodily injury or severe emotional 
//_/_/          distress to any person;
//_/_/    (ii)  invading the personal privacy or violating the human 
//_/_/          rights of any person; or
//_/_/    (iii) committing or preparing for any act of war.
//_/_/
//_/_/ THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND 
//_/_/ CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
//_/_/ INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
//_/_/ MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
//_/_/ DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR 
//_/_/ CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
//_/_/ SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
//_/_/ BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
//_/_/ SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
//_/_/ INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
//_/_/ WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
//_/_/ NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
//_/_/ OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
//_/_/ OF SUCH DAMAGE.
//_/_/ 
//_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/

#ifdef ENABLE_PROTOBUF

#if !defined(_WIN32)
#include <errno.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <poll.h>
#endif

#include "tcp_connection.h"

/**
 * Check if sock is a valid socket. We need this utility function to handle
 * Windows and non-Windows error codes.
 * \param sock The socket descriptor to check.
 * \return True if sock is valid.
 */
static bool
#if defined(_WIN32)
isValidSocket(const SOCKET sock) { return sock != INVALID_SOCKET; }
#else
isValidSocket(const int sock) { return sock >= 0; }
#endif

/**
 * Set sock to the value of an invalid socket. We need this utility function to 
 * handle Windows and non-Windows error codes.
 * \param sock The socket descriptor to set.
 */
static void
#if defined(_WIN32)
setSocketInvalid(SOCKET &sock) { sock = INVALID_SOCKET; }
#else
setSocketInvalid(int &sock) { sock = -1; }
#endif

/**
 * Return the error number which is set when a socket functions returns failure.
 * \return The error number.
 */
static int getLastError()
{
#if defined(_WIN32)
  return WSAGetLastError();
#else
  return errno;
#endif
}

namespace tcp_io_device {

  TCPConnection::TCPConnection(std::shared_ptr<SafeQueue> receive_queue, std::shared_ptr<SafeQueue> send_queue, uint64_t msg_length_buf_size)
  {
    outgoing_queue_ = send_queue;
    incoming_queue_ = receive_queue;
    msg_length_buf_size_ = msg_length_buf_size;
    state_ = NOT_STARTED;
    setSocketInvalid(tcp_socket_);
    setSocketInvalid(server_listen_socket_);
  }

  TCPConnection::~TCPConnection()
  {
    std::cout << "> INFO: Shutting down TCP connection" << std::endl;
    // Set state to STOPPED triggers end of while loop in the backgroundHandler.
    // Wait for the background thread to join and close the socket, if necessary
    state_ = STOPPED;
    tcp_background_thread_->join();
    if (isValidSocket(tcp_socket_)) {
#if defined(_WIN32)
      int err = shutdown(tcp_socket_, SD_BOTH);
#else
      int err = shutdown(tcp_socket_, SHUT_WR);
#endif
      if (err != 0) {
        std::cout << "ERROR: Shutdown of Client Socket failed with error: " << getLastError() << std::endl;
      }
#if defined(_WIN32)
      closesocket(tcp_socket_);
      WSACleanup();
#else
      close(tcp_socket_);
#endif
    }
  }

  int TCPConnection::listenAndAwaitConnection(std::string port)
  {
    port_ = port;

#if defined(_WIN32)
    WSADATA wsa_data;
    int err;

    err = WSAStartup(MAKEWORD(2, 2), &wsa_data);
    if (err != 0) {
      std::cout << "ERROR: WSAStartup failed with error: " << err << std::endl;
      return 1;
    }
    struct addrinfo* result = NULL;
    struct addrinfo hints;

    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    std::cout << "> INFO: Resolving server address and port" << std::endl;
    // Resolve the server address and port
    err = getaddrinfo(NULL, port.c_str(), &hints, &result);
    if (err != 0) {
      std::cout << "ERROR: getaddrinfo failed with error: " << err << std::endl;
      WSACleanup();
      return 1;
    }

    std::cout << "> INFO: Creating socket for connection to client" << std::endl;

    // Create a SOCKET for connecting to client
    server_listen_socket_ = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (!isValidSocket(server_listen_socket_)) {
      std::cout << "ERROR: Socker failed with error: " << getLastError() << std::endl;
      freeaddrinfo(result);
      WSACleanup();
      return 1;
    }

    std::cout << "> INFO: Setting up TCP listening socket" << std::endl;
    // Setup the TCP listening socket
    err = ::bind(server_listen_socket_, result->ai_addr, (int)result->ai_addrlen);
    if (err == SOCKET_ERROR) {
      std::cout << "ERROR: Bind failed with error: " << getLastError() << std::endl;
      freeaddrinfo(result);
      closesocket(server_listen_socket_);
      WSACleanup();
      return 1;
    }

    freeaddrinfo(result);

    // Wait for a client to conenct to the socket.
    err = listen(server_listen_socket_, SOMAXCONN);
    if (err == SOCKET_ERROR) {
      std::cout << "ERROR: Listen failed with error: " << getLastError() << std::endl;
      closesocket(server_listen_socket_);
      WSACleanup();
      return 1;
    }


    std::cout << "> INFO: Waiting to accept client socket on port " << port << std::endl;
    // Accept a client socket
    tcp_socket_ = ::accept(server_listen_socket_, NULL, NULL);
    if (!isValidSocket(tcp_socket_)) {
      std::cout << "ERROR: Accepting client failed with error: " << getLastError() << std::endl;
      closesocket(server_listen_socket_);
      WSACleanup();
      return 1;
    }

    std::cout << "> INFO: TCP connection successfully established" << std::endl;

    socket_type_ = SERVER;
#else
    std::cout << "TODO: Implement TCPConnection::listenAndAwaitConnection for non-Windows" << std::endl;
    return 1;
#endif

    return 0;
  }

  int TCPConnection::establishConnection(std::string host, std::string port) {

    host_ = host;
    port_ = port;

    struct addrinfo* result = NULL, hints;

    int err;

#if defined(_WIN32)
    // Initialize Winsock
    std::cout << "> INFO: Initializing Winsock" << std::endl;
    WSADATA wsaData;
    err = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (err != 0) {
      std::cout << "ERROR: WSAStartup failed with error: " << err << std::endl;
      return 1;
    }
#endif

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    std::cout << "> INFO: Resolving server address and port" << std::endl;
    // Resolve the server address and port
    err = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
    if (err != 0) {
      std::cout << "ERROR: getaddrinfo failed with error: " << err << std::endl;
#if defined(_WIN32)
      WSACleanup();
#endif
      return 1;
    }

    std::cout << "> INFO: Creating socket for connection to server" << std::endl;
    // Create a SOCKET for connecting to server
    tcp_socket_ = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (!isValidSocket(tcp_socket_)) {
      std::cout << "ERROR: Socker failed with error: " << getLastError() << std::endl;
      freeaddrinfo(result);
#if defined(_WIN32)
      WSACleanup();
#endif
      return 1;
    }

    std::cout << "> INFO: Connecting to TCP server" << std::endl;
    // Connect to server.
    while (true) {
      std::cout << "Trying to connect to " << host << ":" << port << std::endl;
      err = connect(tcp_socket_, result->ai_addr, (int)result->ai_addrlen);
      if (err == 0) {
        break;
      }
      std::cout << "Failed to connect to server with error: " << getLastError() << std::endl;
      std::cout << "Trying to reconnect in 1 sec..." << std::endl;
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    freeaddrinfo(result);

    if (!isValidSocket(tcp_socket_)) {
      printf("Unable to connect to server!\n");
#if defined(_WIN32)
      WSACleanup();
#else
      close(tcp_socket_);
#endif
      return 1;
    }

    std::cout << "> INFO: TCP connection successfully established" << std::endl;
    
    socket_type_ = CLIENT;

    return 0;
  }

  void TCPConnection::start() {
    // Start the background thread to handle incoming and outgoing messages.
    state_ = RUNNING;
    tcp_background_thread_ = std::make_shared<std::thread>(&TCPConnection::tcpBackgroundHandler, this);
  }

  void TCPConnection::stop()
  {
    state_ = STOPPED;
  }

  void TCPConnection::tcpBackgroundHandler()
  {

    // Wait for the connection to become alive
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    int error_code = 0;
    while (state_ == RUNNING) {
      if (!isValidSocket(tcp_socket_)) {
        std::cout << "WARNING: Lost TCP connection. Trying to reconnect." << std::endl;
        error_code = 0;
        switch (socket_type_)
        {
        case SERVER:
          if (!isValidSocket(server_listen_socket_)) {
            error_code = listenAndAwaitConnection(port_);
            break;
          }
          std::cout << "INFO: Accepting new client on socket, waiting for connection." << std::endl;
          tcp_socket_ = ::accept(server_listen_socket_, NULL, NULL);
          break;
        case CLIENT:
          error_code = establishConnection(host_, port_);
          break;
        default:
          break;
        }
        if (error_code != 0 || !isValidSocket(tcp_socket_))
        {
          std::cout << "Unable to reconnect... Error: " << getLastError() << " Retrying in 1 sec..." << std::endl;
          std::this_thread::sleep_for(std::chrono::milliseconds(1000));
          continue;
        }
        std::cout << "INFO: Reconnect successfull." << std::endl;
        std::unique_ptr<TCPMessage> reconnect_msg = std::make_unique<TCPMessage>();
        reconnect_msg->set_messagetype(TCPMessage::RECONNECT);
        incoming_queue_->enqueue(std::move(reconnect_msg));
      }
      // First send all data from the queue
      std::unique_ptr<TCPMessage> msg = outgoing_queue_->dequeue();
      while (msg) {
        std::cout << "Sending message of type " << msg->messagetype() << std::endl;
        error_code = sendMessage(std::move(msg));
        if (error_code <= 0) {
          // Error occured while sending message, break the loop and end the thread.
          break;
        }
        msg = std::move(outgoing_queue_->dequeue());
      }

      // Yield to other threads while waiting for input.
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      bool got_error = false;
      while (true) {
        // Check if new data is on the TCP connection to receive
        int ready = receiveIsReady(tcp_socket_);
        if (ready == 0) {
          // No messages on the socket, continue with the handler main loop.
          break;
        }
        else if (ready < 0) {
          // Something went wrong when receiving the message, break the handler, end the thread.
          std::cout << "select() == SOCKET_ERROR error: " << getLastError() << std::endl;
          // got_error = true;
          //closesocket(tcp_socket_);
          setSocketInvalid(tcp_socket_);
          break;
        }
        auto in_msg = receiveMessage();
        if (!in_msg) {
          // Something went wrong when receiving the message, break the handler, end the thread.
          // got_error = true;
          //closesocket(tcp_socket_);
          setSocketInvalid(tcp_socket_);
          break;
        }

        // Add it to the queue, let the main thread handle them
        incoming_queue_->enqueue(std::move(in_msg));
      }
      if (got_error)
        break;
    }
    // Clear all entries of the queues before shutting down.
    incoming_queue_->clear();
    outgoing_queue_->clear();

    // Close the socket
    if (isValidSocket(tcp_socket_)) {
#if defined(_WIN32)
      int err = shutdown(tcp_socket_, SD_SEND);
#else
      int err = shutdown(tcp_socket_, SHUT_WR);
#endif
      if (err != 0) {
        std::cout << "ERROR: Shutdown of Client Socket failed with error: " << getLastError() << std::endl;
      }
#if defined(_WIN32)
      closesocket(tcp_socket_);
      WSACleanup();
#else
      close(tcp_socket_);
#endif
    }
    setSocketInvalid(tcp_socket_);
  }

  std::unique_ptr<TCPMessage> TCPConnection::receiveMessage()
  {
    // Number of bytes received
    int received_bytes = 0;

    // Length of read bytes in total (used to ensure split messages are read correctly)
    uint64_t len_res = 0;

    // First read the length of the message to expect (8 byte uint64_t)
    std::string tcp_msg_len_recv_buf;
    tcp_msg_len_recv_buf.reserve(msg_length_buf_size_);
    // To ensure split message is read correctly
    while (len_res < msg_length_buf_size_) {
      received_bytes = recv(tcp_socket_, &(tcp_msg_len_recv_buf[len_res]), msg_length_buf_size_ - len_res, 0);
      if (received_bytes > 0) {
        // All good
        len_res += received_bytes;
      }
      else if (received_bytes == 0) {
        // Client closed the connection
        len_res = -1;
        std::cout << "Connection closing..." << std::endl;
        return NULL;
      }
      else {
        // Error occured during receiving
        std::cout << "recv failed during recv of data length with error: " << getLastError() << std::endl;
#if defined(_WIN32)
        closesocket(tcp_socket_);
        WSACleanup();
#else
        close(tcp_socket_);
#endif
        return NULL;
      }
    }

    // Convert the read bytes to uint64_t. Little Endian! Otherwise invert for loop - not implemented, yet.
    uint64_t msg_len = 0;
    for (int i = msg_length_buf_size_ - 1; i >= 0; --i)
    {
      msg_len <<= 8;
      msg_len |= (unsigned char)tcp_msg_len_recv_buf[i];
    }

    // Reset read bytes and total read bytes to 0
    received_bytes = 0;
    len_res = 0;
    // Read as many packages as needed to fill the message buffer. Ensures split messages are received correctly.
    char* buf = new char[msg_len]();
    while (len_res < msg_len) {
      received_bytes = recv(tcp_socket_, &buf[len_res], msg_len - len_res, 0);
      if (received_bytes > 0) {
        len_res += received_bytes;
      }
      else if (received_bytes == 0) {
        return NULL;
      }
      else {
        std::cout << "recv failed during recv of data message with error: " << getLastError() << std::endl;
        return NULL;
      }
    }

    // Parse the byte-stream into a TCPMessage
    std::unique_ptr<TCPMessage> msg = std::make_unique<TCPMessage>();
    if (!msg->ParseFromArray(buf, msg_len)) {
      std::cout << "ERROR: Parsing Message from String failed" << std::endl;
      return NULL;
    }

    delete[] buf;

    return msg;
  }

  int TCPConnection::sendMessage(std::unique_ptr<TCPMessage> msg)
  {
    // Serialize the TCPMessage
    std::string out;
    out = msg->SerializeAsString();

    // First put the length of the message in the first 8 bytes of the output stream
    std::string out_buffer = "";
    for (int i = 0; i < 8; ++i) {
      out_buffer += (unsigned char)((int)(((uint64_t)out.size() >> (i * 8)) & 0xFF));
    }

    // Attach the serialized message to the byte-stream
    out_buffer += out;

    // Send message length + message through the socket.
    int i_send_result = ::send(tcp_socket_, &out_buffer[0], out_buffer.size(), 0);
    if (i_send_result < 0) {
      std::cout << "SendMessage failed with error: " << getLastError() << std::endl;
#if defined(_WIN32)
      closesocket(tcp_socket_);
      WSACleanup();
#else
      close(tcp_socket_);
#endif
    }

    return i_send_result;
  }

#if defined(_WIN32)
  int TCPConnection::receiveIsReady(SOCKET fd)
#else
  int TCPConnection::receiveIsReady(int fd)
#endif
  {
    if (!isValidSocket(fd))
      // The socket is not open. Just silently return.
      return 0;

#if defined(_WIN32)
    timeval tv{ 0, 0 };
    FD_SET tcp_client_fd_set;
    FD_ZERO(&tcp_client_fd_set);
    FD_SET(fd, &tcp_client_fd_set);
    int rc = ::select(fd + 1, &tcp_client_fd_set, NULL, NULL, &tv);
#else
    struct pollfd pollInfo[1];
    pollInfo[0].fd = fd;
    pollInfo[0].events = POLLIN;
    int rc = ::poll(pollInfo, 1, 0);
#endif
    if (rc < 0) {
      return -1;
    }
    if (rc == 0) {
      // No messages on the socket.
      return 0;
    }
#if !defined(_WIN32)
    if (!(pollInfo[0].revents & POLLIN)) {
      // No POLLIN flag.
      return 0;
    }
#endif

    return 1;
  }

  std::map<int, std::string> TCPConnection::type_to_name_map_ =
  { {TCPMessage_Type_DATA, "DATA"},
    {TCPMessage_Type_SETUP, "SETUP"},
    {TCPMessage_Type_START, "START"},
    {TCPMessage_Type_STOP, "STOP"} };

} // namespace tcp_io_device

#endif
