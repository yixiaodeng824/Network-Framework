#include "MessageQueue.h"

MessageQueue::MessageQueue(size_t capacity) : capacity_(capacity) {}

void MessageQueue::push(Task task) {
	{
		std::unique_lock<std::mutex> lck(mtx_);
		// 有界且已满时阻塞;队列一旦关闭则不再等待,直接入队(排空阶段保证被消费)
		while (capacity_ > 0 && m_tasks.size() >= capacity_ &&
		       !m_closed.load(std::memory_order_relaxed)) {
			m_cv.wait(lck);
		}
		m_tasks.push_back(std::move(task));
	}
	m_cv.notify_one();
}

bool MessageQueue::pushOverrunOldest(Task task) {
	{
		std::lock_guard<std::mutex> lck(mtx_);
		if (m_closed.load(std::memory_order_relaxed)) {
			return false; // 已关闭,新消息直接丢弃
		}
		if (capacity_ > 0 && m_tasks.size() >= capacity_) {
			// 队列满:丢弃最旧,给新消息腾位置。永不阻塞,保证生产者不被拖垮
			m_tasks.pop_front();
			m_dropped.fetch_add(1, std::memory_order_relaxed);
		}
		m_tasks.push_back(std::move(task));
	}
	m_cv.notify_one();
	return true;
}

bool MessageQueue::pop(Task& task) {
	std::unique_lock<std::mutex> lck(mtx_);
	while (m_tasks.empty()) {
		if (m_closed.load(std::memory_order_relaxed)) {
			return false;
		}
		m_cv.wait(lck);
	}
	task = std::move(m_tasks.front());
	m_tasks.pop_front();
	if (capacity_ > 0) {
		// 可能有生产者在 push 里因队列满而等待,腾出位置后叫醒一个
		m_cv.notify_one();
	}
	return true;
}

bool MessageQueue::tryPop(Task& task) {
	std::lock_guard<std::mutex> lck(mtx_);
	if (m_tasks.empty()) {
		return false;
	}
	task = std::move(m_tasks.front());
	m_tasks.pop_front();
	if (capacity_ > 0) {
		m_cv.notify_one();
	}
	return true;
}

void MessageQueue::close() {
	m_closed.store(true, std::memory_order_relaxed);
	m_cv.notify_all();
}

size_t MessageQueue::size() const {
	std::lock_guard<std::mutex> lck(mtx_);
	return m_tasks.size();
}

size_t MessageQueue::droppedCount() const {
	return m_dropped.load(std::memory_order_relaxed);
}
