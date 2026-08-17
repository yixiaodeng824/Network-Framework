#pragma once
#include <string>
#include <ctime>
enum class SendResult {
	SentAll,    // 发完了
	Pending,    // 没发完,等 EPOLLOUT 续发
	Error,      // 连接出错,该断开
};

class Connection {
public:
	Connection() = default;
	Connection(int fd) : fd_(fd), recv_buffer_(""), send_buffer_(""), write_waiting_(false),closing_(false),last_active_(time(nullptr)) {
	}
	void appendRecv(const char* data, size_t len) {
		recv_buffer_.append(data, len);
	}
	void processBufferedData();
	SendResult sendReadyMessage();
	void markClosing() { closing_ = true; }//标记正在关闭
	bool isClosing()const { return closing_; }//判断是否正在关闭
	void touchActive(){ last_active_ = time(nullptr); }//收到数据就刷新活动时间
	bool isTimeout(time_t now, int sec)const { return now - last_active_ >= sec; }//计算是否超时
private:
	int fd_;
	std::string recv_buffer_;
	std::string send_buffer_;
	bool write_waiting_{false};//这里不能使用原子类型，因为connection类需要进map，要进行复制
	bool closing_{ false };//表示正在关闭中，等待缓冲排空再关闭
	time_t last_active_{ 0 };// 最后收到数据的时间,心跳踢人用
};