#pragma once
#include <cstddef>
class MemoryPool{
public:
    MemoryPool(size_t block_size, size_t block_count); // 构造：预分配 + 串成链
    ~MemoryPool();                                     // 析构：释放整块内存

    void *alloc();        // 取一块（O(1)）
    void free(void *ptr); // 还一块（O(1)）
    bool is_in_pool(void* p){
        return p >= memory_ && p < memory_ + block_count_ * block_size_;
    }//判断指针是否在内存池范围内，如果是释放的时候还内存池，不是就delete

private:
    size_t block_size_;
    size_t block_count_;
    char* memory_;//预分配的大内存
    void* free_head_;//空间链表头
};