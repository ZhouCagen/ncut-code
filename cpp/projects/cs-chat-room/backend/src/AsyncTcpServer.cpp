#include "AsyncTcpServer.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string>
#include <utility>

namespace
{
constexpr int maxEpollEvents = 64;
constexpr std::size_t bufferSize = 1024;
constexpr std::size_t maxMessageSize = 64 * 1024;

std::runtime_error makeSystemError(const std::string &message)
{
    return std::runtime_error(message + ": " + std::strerror(errno));
}
} // namespace

AsyncTcpServer::AsyncTcpServer(std::uint16_t port) : port_(port)
{
}
AsyncTcpServer::~AsyncTcpServer()
{
    for (const auto &pair : inputBuffers_)
        ::close(pair.first);

    inputBuffers_.clear();
    outputBuffers_.clear();

    if (listenSocket_ != -1)
        ::close(listenSocket_);

    if (epollInstance_ != -1)
        ::close(epollInstance_);
}

void AsyncTcpServer::start()
{
    createSocket();
    setReuseAddress();
    setNonBlocking(listenSocket_);
    bindAddress();
    listenOnSocket();

    createEpoll();
    addToEpoll(listenSocket_, EPOLLIN | EPOLLET);
    spdlog::info("async tcp server started on port {}", port_);

    eventLoop();
}

void AsyncTcpServer::eventLoop()
{
    epoll_event events[maxEpollEvents]{};
    while (true)
    {
        int readyCount = epoll_wait(epollInstance_, events, maxEpollEvents, -1);
        if (readyCount == -1)
        {
            if (errno == EINTR) // Interrupted system call
                continue;

            throw makeSystemError("epoll_wait failed");
        }

        for (int i = 0; i < readyCount; i++)
        {
            int socketFd = events[i].data.fd;
            std::uint32_t eventFlags = events[i].events;

            if (socketFd == listenSocket_)
            {
                handleAccept();
                continue;
            }

            if (eventFlags & EPOLLERR)
            {
                closeClient(socketFd);
                continue;
            }

            if (eventFlags & EPOLLIN)
            {
                handleRead(socketFd);
                if (inputBuffers_.find(socketFd) == inputBuffers_.end())
                    continue;
            }
            if (eventFlags & EPOLLOUT)
            {
                handleWrite(socketFd);
                if (outputBuffers_.find(socketFd) == outputBuffers_.end())
                    continue;
            }
            if (eventFlags & (EPOLLHUP | EPOLLRDHUP))
            {
                closeClient(socketFd);
                continue;
            }
        }
    }
}

void AsyncTcpServer::setClientConnectedCallback(std::function<void(int)> callback)
{
    clientConnectedCallback_ = std::move(callback);
}

void AsyncTcpServer::setClientDisconnectedCallback(std::function<void(int)> callback)
{
    clientDisconnectedCallback_ = std::move(callback);
}

void AsyncTcpServer::setInputBufferCallback(std::function<void(int, std::string &)> callback)
{
    inputBufferCallback_ = std::move(callback);
}

void AsyncTcpServer::enqueueWrite(int clientSocket, const std::string &data)
{
    if (data.empty())
        return;

    auto outputIterator = outputBuffers_.find(clientSocket);
    if (outputIterator == outputBuffers_.end())
    {
        spdlog::warn("output buffer not found for socket {}", clientSocket);
        return;
    }

    if (outputIterator->second.size() + data.size() > maxMessageSize)
    {
        spdlog::warn("client socket {} message too large", clientSocket);
        closeClient(clientSocket);
        return;
    }

    outputIterator->second.append(data);

    modifyEpoll(clientSocket, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
}

void AsyncTcpServer::closeClient(int clientSocket)
{
    if (clientSocket == -1)
        return;

    bool knownClient = inputBuffers_.find(clientSocket) != inputBuffers_.end() ||
                       outputBuffers_.find(clientSocket) != outputBuffers_.end();

    if (knownClient && clientDisconnectedCallback_)
        clientDisconnectedCallback_(clientSocket);

    inputBuffers_.erase(clientSocket);
    outputBuffers_.erase(clientSocket);

    if (epollInstance_ != -1)
        removeFromEpoll(clientSocket);

    int closeResult = ::close(clientSocket);
    if (closeResult == -1)
    {
        spdlog::warn("close failed for socket {}: {}", clientSocket, std::strerror(errno));
        return;
    }

    spdlog::info("client socket {} closed successfully", clientSocket);
}

void AsyncTcpServer::createSocket()
{
    listenSocket_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenSocket_ == -1)
        throw makeSystemError("socket failed");

    spdlog::info("listen socket created successfully");
}

void AsyncTcpServer::setReuseAddress()
{
    int opt = 1;
    int setSockOptResult = ::setsockopt(listenSocket_, SOL_SOCKET, SO_REUSEADDR, &opt,
                                        static_cast<socklen_t>(sizeof(opt)));
    if (setSockOptResult == -1)
        throw makeSystemError("setsockopt failed");

    spdlog::info("SO_REUSEADDR set successfully");
}

void AsyncTcpServer::setNonBlocking(int socketFd)
{
    int currentFlags = ::fcntl(socketFd, F_GETFL, 0);
    // 获取这个 fd 当前的状态标志
    if (currentFlags == -1)
        throw makeSystemError("fcntl F_GETFL failed");

    int setResult = ::fcntl(socketFd, F_SETFL, currentFlags | O_NONBLOCK);
    // 设置这个 fd 的状态标志
    if (setResult == -1)
        throw makeSystemError("fcntl F_SETFL O_NONBLOCK failed");
}

void AsyncTcpServer::bindAddress()
{
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serverAddr.sin_port = htons(port_);

    int bindResult = ::bind(listenSocket_, reinterpret_cast<sockaddr *>(&serverAddr),
                            static_cast<socklen_t>(sizeof(serverAddr)));

    if (bindResult == -1)
        throw makeSystemError("bind failed");

    spdlog::info("server bound successfully on port {}", port_);
}

void AsyncTcpServer::listenOnSocket()
{
    int listenResult = ::listen(listenSocket_, SOMAXCONN);

    if (listenResult == -1)
        throw makeSystemError("listen failed");

    spdlog::info("server listened successfully");
}

void AsyncTcpServer::createEpoll()
{
    epollInstance_ = ::epoll_create1(EPOLL_CLOEXEC);
    // 给这个 epoll 文件描述符设置 close-on-exec 标志

    if (epollInstance_ == -1)
        throw makeSystemError("epoll_create1 failed");

    spdlog::info("epoll instance created successfully");
}

void AsyncTcpServer::addToEpoll(int socketFd, std::uint32_t events)
{
    epoll_event event{};
    event.events = events;
    event.data.fd = socketFd;

    int addResult = ::epoll_ctl(epollInstance_, EPOLL_CTL_ADD, socketFd, &event);

    if (addResult == -1)
        throw makeSystemError("epoll_ctl add failed");

    spdlog::info("socket {} added to epoll successfully", socketFd);
}

void AsyncTcpServer::modifyEpoll(int socketFd, std::uint32_t events)
{
    epoll_event event{};
    event.events = events;
    event.data.fd = socketFd;

    int modifyResult = ::epoll_ctl(epollInstance_, EPOLL_CTL_MOD, socketFd, &event);

    if (modifyResult == -1)
        throw makeSystemError("epoll_ctl modify failed");

    spdlog::info("socket {} modified in epoll successfully", socketFd);
}

void AsyncTcpServer::removeFromEpoll(int socketFd)
{
    if (epollInstance_ == -1 || socketFd == -1)
        return;

    int removeResult = ::epoll_ctl(epollInstance_, EPOLL_CTL_DEL, socketFd, nullptr);

    if (removeResult == -1)
    {
        spdlog::error("epoll_ctl remove failed for socket {}: {}", socketFd, std::strerror(errno));
        return;
    }
    spdlog::info("socket {} removed from epoll successfully", socketFd);
}

void AsyncTcpServer::handleAccept()
{
    while (true)
    {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientSocket =
            ::accept(listenSocket_, reinterpret_cast<sockaddr *>(&clientAddr), &clientLen);
        if (clientSocket == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK) // 现在没东西了，或者现在做不了，等下再试
                break;

            if (errno == EINTR) // 系统调用被信号打断了
                continue;

            throw makeSystemError("accept failed");
        }

        setNonBlocking(clientSocket);

        inputBuffers_[clientSocket] = "";
        outputBuffers_[clientSocket] = "";

        addToEpoll(clientSocket, EPOLLIN | EPOLLRDHUP | EPOLLET);
        // 默认 epoll 是 LT 模式，也就是水平触发 EPOLLET 以后，就是 ET 模式
        // ET：只在状态变化那一刻提醒你一次
        // LT: 只要有数据就提醒你

        if (clientConnectedCallback_)
            clientConnectedCallback_(clientSocket);

        char clientIp[INET_ADDRSTRLEN]{};
        const char *ipResult =
            ::inet_ntop(AF_INET, &clientAddr.sin_addr, clientIp, INET_ADDRSTRLEN);
        if (ipResult == nullptr)
            throw makeSystemError("inet_ntop failed");

        uint16_t clientPort = ntohs(clientAddr.sin_port);
        spdlog::info("client connected: {}:{}, socket {}", clientIp, clientPort, clientSocket);
    }
}

void AsyncTcpServer::handleRead(int clientSocket)
{
    char buffer[bufferSize];
    while (true)
    {
        ssize_t recvLen = ::recv(clientSocket, buffer, sizeof(buffer), 0);
        if (recvLen > 0)
        {
            auto inputIterator = inputBuffers_.find(clientSocket);
            if (inputIterator == inputBuffers_.end())
            {
                spdlog::warn("ignore readable event for unknown or closed socket {}", clientSocket);
                return;
            }

            inputIterator->second.append(buffer, static_cast<std::size_t>(recvLen));
            if (inputIterator->second.size() > maxMessageSize)
            {
                spdlog::warn("client socket {} message too large", clientSocket);
                closeClient(clientSocket);
                return;
            }

            processInputBuffer(clientSocket);

            if (inputBuffers_.find(clientSocket) == inputBuffers_.end())
                return;

            continue;
        }
        if (recvLen == 0)
        {
            spdlog::info("client socket {} closed connection", clientSocket);
            closeClient(clientSocket);
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            break;

        if (errno == EINTR)
            continue;

        spdlog::error("recv failed on socket {}: {}", clientSocket, std::strerror(errno));
        closeClient(clientSocket);
        return;
    }
}

void AsyncTcpServer::handleWrite(int clientSocket)
{
    auto outputIterator = outputBuffers_.find(clientSocket);
    if (outputIterator == outputBuffers_.end())
    {
        spdlog::warn("output buffer not found for socket {}", clientSocket);
        return;
    }

    std::string &outputBuffer = outputIterator->second;

    while (!outputBuffer.empty())
    {
        ssize_t sendLen =
            ::send(clientSocket, outputBuffer.data(), outputBuffer.size(), MSG_NOSIGNAL);
        // 客户端 close 了 服务器还 send 进程发一个信号 SIGPIPE 直接终止整个程序
        // 防止服务器因为给已断开的客户端 send 而被 SIGPIPE 信号干死
        if (sendLen > 0)
        {
            outputBuffer.erase(0, static_cast<size_t>(sendLen));
            continue;
        }

        if (sendLen == 0)
            break;

        if (errno == EAGAIN || errno == EWOULDBLOCK) // socket 发送缓冲区暂时满了
        {
            modifyEpoll(clientSocket, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
            return;
        }

        if (errno == EINTR)
            continue;

        spdlog::error("send failed on socket {}: {}", clientSocket, std::strerror(errno));
        closeClient(clientSocket);
        return;
    }

    if (outputBuffer.empty())
        modifyEpoll(clientSocket, EPOLLIN | EPOLLRDHUP | EPOLLET);
}

void AsyncTcpServer::processInputBuffer(int clientSocket)
{
    auto inputIterator = inputBuffers_.find(clientSocket);
    if (inputIterator == inputBuffers_.end())
    {
        spdlog::warn("input buffer not found for socket {}", clientSocket);
        return;
    }

    if (!inputBufferCallback_)
        return;

    try
    {
        inputBufferCallback_(clientSocket, inputIterator->second);
    }
    catch (const std::exception &error)
    {
        spdlog::warn("input buffer callback failed, socket {}, error: {}", clientSocket,
                     error.what());
        closeClient(clientSocket);
    }
}
