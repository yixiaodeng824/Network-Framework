#pragma once
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>

class MessageQueue
{
public:
	using Task = std::function<void()>;

	// capacity == 0 表示无上限(线程池等场景保持原行为);
	// capacity > 0 表示有界队列,满时的行为由 push / pushOverrunOldest 决定
	explicit MessageQueue(size_t capacity = 0);

	void push(Task task);            // 生产者:塞一个回调进去;有界且已满时阻塞等待(pop 腾出空间)
	bool pushOverrunOldest(Task task); // 生产者:永不阻塞;有界且已满时丢弃最旧消息再入队;
	                                   //         队列已关闭时直接丢弃并返回 false
	bool pop(Task& task);            // 消费者:阻塞等任务,取出→true;被关闭且空→false
	bool tryPop(Task& task);         // 消费者:非阻塞取,空或已关闭→false
	void close();                    // 唤醒所有正在 pop 等待的线程,排空后退出任务
	size_t size() const;             // 当前队列深度
	size_t droppedCount() const;     // 因容量满被丢弃(丢最旧)的消息总数

private:
	mutable std::mutex mtx_;//队列锁
	std::condition_variable m_cv;            // 通知 #2:叫醒睡觉的 worker
	std::deque<Task> m_tasks;                // 被保护的共享数据
	std::atomic<bool> m_closed{ false };
	std::atomic<size_t> m_dropped{ 0 };
	size_t capacity_;                        // 0 = 无上限
};

