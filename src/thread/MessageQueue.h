#pragma once
#include <functional>
#include <mutex>
#include <deque>
#include <condition_variable>
#include <atomic>
class MessageQueue
{
public:
	using Task = std::function<void()>;
	void push(std::function<void()> task);   // 生产者用:塞一个回调进去
	bool pop(std::function<void()>& task);   // 消费者用:阻塞等任务,取出→true;被关闭且空→false
	void close();                       // 唤醒所有正在 pop 等待的线程,退出任务
private:
	std::mutex mtx_;//队列锁
	std::condition_variable m_cv;            // 通知 #2:叫醒睡觉的 worker
	std::deque<std::function<void()>> m_tasks;  // 被保护的共享数据
	std::atomic<bool> m_closed{ false };
};

