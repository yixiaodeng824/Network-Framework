#pragma once
#include "memory_pool.h"
#include <type_traits>
#include <cstddef>
#include <new>

// PoolAllocator：让 std::string 用上 MemoryPool 的自定义分配器
// 核心规则（跟你的内存池一致）：
//   allocate(n)：n*sizeof(T) <= 128B  → 从池取一块；否则 new
//   deallocate(p)：p 在池范围内 → 还池；否则 delete
template<typename T>
class PoolAllocator {
public:
    using value_type = T;                                   // ① 这个 allocator 管什么类型的元素
    using propagate_on_container_move_assignment = std::true_type;  // ② move 时分配器跟着容器走

    PoolAllocator(MemoryPool* pool) : pool_(pool) {}        // ③ 构造：记住池，后面分配/释放都要用它

    // ④ rebind：string 内部可能要其他类型的
    PoolAllocator() : pool_(nullptr)
    {
    }
    template <typename U>
    PoolAllocator(const PoolAllocator<U>& other) : pool_(other.pool()) {}   

    T* allocate(size_t n) {                                 // ⑤ 核心：string 要 n 个 T 的内存
        size_t bytes = n * sizeof(T);
        if (bytes <= kBlockSize&&pool_)                            // ≤128B → 从池取一块
            return static_cast<T*>(pool_->alloc());
        return static_cast<T*>(::operator new(bytes));      // >128B → 走 new
    }

    void deallocate(T* p, size_t /*n*/) {                   // ⑥ 核心：释放一块
        if (pool_ && pool_->is_in_pool(p))                           // p 在池里 → 还池
            pool_->free(p);
        else                                                // 是 new 的 → delete
            ::operator delete(p);
    }

    MemoryPool* pool() const { return pool_; }              // 供 rebind 和比较用

    template<typename U>
    bool operator==(const PoolAllocator<U>& o) const { return pool_ == o.pool(); }
    template<typename U>
    bool operator!=(const PoolAllocator<U>& o) const { return pool_ != o.pool(); }

private:
    MemoryPool* pool_;
    static constexpr size_t kBlockSize = 128;               // 跟池的块大小保持一致
};
