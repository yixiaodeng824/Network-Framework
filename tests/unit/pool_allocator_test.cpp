// PoolAllocator 测试：验证 string 用内存池分配
#include "pool_allocator.h"
#include <cassert>
#include <cstdio>
#include <string>

int main() {
    MemoryPool pool(128, 10);
    PoolAllocator<char> alloc(&pool);
    using PoolString = std::basic_string<char, std::char_traits<char>, PoolAllocator<char>>;

    // ① 中等字符串（超过 SSO 且 ≤128B）→ 应该从池取
    PoolString small(alloc);
    small.assign(20, 'a');                 // 20 字节：超过 SSO，且 ≤128
    assert(pool.is_in_pool((void*)small.data()));
    printf("中等字符串走线程池 ok\n");

    // ② 大字符串（>128B）→ 应该走 new
    PoolString big(alloc);
    big.assign(1000, 'b');                 // 1000 字节 > 128
    assert(!pool.is_in_pool((void*)big.data()));
    printf("大字符串走 new ok\n");

    // ③ 反复创建销毁，验证池能复用、不崩
    for (int i = 0; i < 100; i++) {
        PoolString s(alloc);
        s.assign(30, 'c');                 // 30 字节：走池
        assert(pool.is_in_pool((void*)s.data()));
    }
    printf("PoolAllocator all success\n");
    return 0;
}
