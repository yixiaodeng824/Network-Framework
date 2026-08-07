#pragma once
//最底层工作线程
  // work_thread.h
#pragma once
#include <thread>
#include <atomic>
//本类只负责处理工作，所有分发工作由队列进行
#include <functional>
#include "MessageQueue.h"
class MessageQueue;   // 前置声明:只依赖队列的接口,不依赖实现

class WorkThread {
public:
    explicit WorkThread(MessageQueue* queue);  // 构造即启动线程
    ~WorkThread();                             // 请求停止并 join

    // std::thread 不可拷贝,所以这类也要禁掉拷贝,否则编译报错
    WorkThread(const WorkThread&) = delete;
    WorkThread& operator=(const WorkThread&) = delete;
    void join();   // 等待线程结束

private:
    void run();                 // 线程入口
    MessageQueue* m_queue; // 任务来源
    std::thread        m_thread;
};

