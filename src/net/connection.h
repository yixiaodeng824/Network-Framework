#pragma once
#include <string>
enum class SendResult {
	SentAll,    // 发完了
	Pending,    // 没发完,等 EPOLLOUT 续发
	Error,      // 连接出错,该断开
};

class Connection {
public:
	Connection() = default;
	Connection(int fd) : fd_(fd), recv_buffer_(""), send_buffer_(""), write_waiting_(false) {
	}
	void appendRecv(const char* data, size_t len) {
		recv_buffer_.append(data, len);
	}
	void processBufferedData();
	SendResult sendReadyMessage();

private:
	int fd_;
	std::string recv_buffer_;
	std::string send_buffer_;
	bool write_waiting_{false};//这里不能使用原子类型，因为connection类需要进map，要进行复制
};