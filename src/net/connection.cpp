#include "connection.h"
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <cstdint>
#include <cerrno>
using namespace std;



void Connection::processBufferedData() {
	while (true) {
		if (recv_buffer_.length() < 4)	break;
		uint32_t len;
		memcpy(&len, recv_buffer_.data(), 4);
		len = ntohl(len);
		if (recv_buffer_.length() < 4 + len) break;

		string msg = recv_buffer_.substr(4, len);
		uint32_t resp_len = htonl(msg.size());
		send_buffer_.append(reinterpret_cast<const char*>(&resp_len), 4);
		send_buffer_.append(msg);	
		recv_buffer_.erase(0, 4 + len);
	}
}

SendResult Connection::sendReadyMessage() {
	if (send_buffer_.empty()) return SendResult::SentAll;
	int n = send(fd_, send_buffer_.data(), send_buffer_.size(), 0);
	if (n < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return SendResult::Pending;
		}
		return SendResult::Error;
	}
	if (n > 0) send_buffer_.erase(0, n);  //如果有消息 
	if (!send_buffer_.empty())	write_waiting_ = true;
	return send_buffer_.empty() ? SendResult::SentAll : SendResult::Pending;
}