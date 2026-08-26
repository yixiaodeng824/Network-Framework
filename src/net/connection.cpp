#include "connection.h"
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <cstdint>
#include <cerrno>
#include <strings.h> // strncasecmp
#include <cstdlib>   // atoi
using namespace std;



std::vector<PoolString> Connection::processBufferedData() {
	vector<PoolString>	afterPackageResult;
	while (true) {
		size_t remain = recv_buffer_.size() - recv_idx_;
		if (remain < 4)	break;
        const char *data = recv_buffer_.data() + recv_idx_;

        bool is_HTTP = (memcmp(data, "GET ", 4) == 0 || memcmp(data, "POST", 4)==0 || memcmp(data, "HEAD", 4)==0 || memcmp(data, "PUT ", 4)==0);
        if(is_HTTP){
            const char* header_end = nullptr;
            for (size_t i = 0; i + 3 < remain;i++){
                if(data[i]=='\r'&&data[i+1]=='\n'&&data[i+2]=='\r'&&data[i+3]=='\n'){
                    header_end = data + i + 4;break;
                }
                
            }
            if(header_end==nullptr) break;//半包
            size_t header_len = header_end - data;
            //解析content-len
            size_t content_len = 0;
            for (size_t i = 0; i + 14 < remain;i++){
                if(strncasecmp(data+i,"Content-Length",14)==0){
                    content_len = atoi(data + i + 15);
                    break;
                }
            }
            if(content_len>kMaxFrameSize){
                frame_error_ = true;
                recv_buffer_.clear();
                recv_idx_ = 0;
                break;
            }
            if(remain < content_len + header_len)   break;//半包
            PoolString msg(recv_buffer_.data() + recv_idx_, content_len + header_len, PoolAllocator<char>(memory_pool_));
            afterPackageResult.push_back(std::move(msg));
            recv_idx_ += (header_len + content_len);
        }
        else{
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

            PoolString msg(recv_buffer_.data() + recv_idx_ + 4, len, PoolAllocator<char>(memory_pool_));

            afterPackageResult.push_back(std::move(msg));
            recv_idx_ += (4 + len);
        }
        
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

void Connection::sendMsgToSendBuf(string_view msg) {
	if (send_buffer_.size() + msg.size() > kSendHighWaterMark) {
		send_overflow = true;
		return;
	}
	uint32_t resp_len = htonl(msg.size());
	send_buffer_.append(reinterpret_cast<const char*>(&resp_len), 4);
	send_buffer_.append(msg.data(),msg.size());
}