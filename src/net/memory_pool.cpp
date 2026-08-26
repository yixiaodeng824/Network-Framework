#include <memory_pool.h>
using namespace std;
MemoryPool::MemoryPool(size_t block_size, size_t block_count)
:block_size_(block_size),block_count_(block_count){
    memory_ = new char[block_count_ * block_size_];
    char *cur = memory_;
    for (size_t i = 0; i < block_count_;i++){
        char *next = i+1<block_count_ ? cur + block_size_ : nullptr;
        *reinterpret_cast<void**>(cur) = next;
        cur += block_size_;
    }
    free_head_ = memory_;
}

MemoryPool::~MemoryPool(){
    if(memory_!=nullptr){
        delete[] memory_;
        memory_ = nullptr;
    }
    free_head_ = nullptr;
}

void * MemoryPool::alloc(){
    if(free_head_==nullptr)
        return nullptr;
    void *block = free_head_;
    free_head_ = *reinterpret_cast<void**>(block);
    return block;
}

void MemoryPool::free(void* ptr){
    *reinterpret_cast<void **>(ptr) = free_head_;
    free_head_ = ptr;
}