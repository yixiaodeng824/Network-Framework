#include "connection.h"
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <cstdint>
#include <cerrno>
using namespace std;



std::vector<std::string> Connection::processBufferedData() {
	vector<string>	afterPackageResult;
	while (true) {
		size_t remain = recv_buffer_.size() - recv_idx_;
		if (remain < 4)	break;
		uint32_t len;
		memcpy(&len, recv_buffer_.data() + recv_idx_, 4);
		len = ntohl(len);
		if (len > kMaxFrameSize){
			frame_error_ = true;//应该要关闭连接
			recv_buffer_.clear();
			recv_idx_ = 0;
			break;
		}
		if (remain < 4 + len) break;

		string msg = recv_buffer_.substr(recv_idx_ + 4, len);
		
		afterPackageResult.push_back(msg);
		recv_idx_ += (4 + len);
	}

	//空间清理
	if (recv_idx_ > 0) {
		if (recv_idx_ == recv_buffer_.size()) {
			recv_buffer_.clear();//不动内存
			recv_idx_ = 0;
		}
		else if (recv_idx_ > 1024) {
			recv_buffer_.erase(0, recv_idx_);
			recv_idx_ = 0;
		}
	}
	return afterPackageResult;
}

SendResult Connection::sendReadyMessage() {
	if (hasSendError() == true)	return SendResult::Error;
	if (send_buffer_.empty()) {
		send_idx_ = 0;
		return SendResult::SentAll;
	}
	int n = send(fd_, send_buffer_.data() + send_idx_, send_buffer_.size() - send_idx_, 0);
	if (n < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return SendResult::Pending;
		}
		return SendResult::Error;	
	}
	send_idx_ += n;
	if (send_idx_ == send_buffer_.size()) {
		send_buffer_.clear();
		send_idx_ = 0;
		return SendResult::SentAll;
	}
	//send_buffer_.erase(0, n);  //如果有消息 
	write_waiting_ = true;
	return SendResult::Pending;
}

void Connection::sendMsgToSendBuf(const std::string& msg) {
	if (send_buffer_.size() + msg.size() > kSendHighWaterMark) {
		send_overflow = true;
		return;
	}
	uint32_t resp_len = htonl(msg.size());
	send_buffer_.append(reinterpret_cast<const char*>(&resp_len), 4);
	send_buffer_.append(msg);
}