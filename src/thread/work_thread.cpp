#include "work_thread.h"
#include <iostream>
#include <mutex>
#include <exception>
using namespace std;
WorkThread::WorkThread(MessageQueue* queue) :m_queue(queue),m_thread(&WorkThread::run,this){
}

WorkThread::~WorkThread() {
	this->join();
}// 请求停止并 join


void WorkThread::join() {
	if (m_thread.joinable()) {
		m_thread.join();
	}
}

void WorkThread::run() {
	while (true) {
		{
			MessageQueue::Task task;
			if (!m_queue->pop(task)) break;
			try { 
				task(); 
			}
			catch (exception& e) {
				cerr << "异常是" << e.what();
			}
			catch (...) {
				cerr << "未知异常";
			}
		}	
	}
}