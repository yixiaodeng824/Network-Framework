#include "MessageQueue.h"
using namespace std;
void MessageQueue::push(std::function<void()> task) {
	{
		unique_lock<mutex> lck(mtx_);
		this->m_tasks.push_back(task);
	}
	m_cv.notify_one();
}

bool MessageQueue::pop(std::function<void()>& task) {
	
	unique_lock<mutex> lck(mtx_);
	while (this->m_tasks.empty()) {
		if (m_closed == true){
			return false;
		}
		m_cv.wait(lck);
	}
	task = move(m_tasks.front());
	m_tasks.pop_front();
	return true;
}

void MessageQueue::close() {
	m_closed = true;
	m_cv.notify_all();
}
