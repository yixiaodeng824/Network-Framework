// 内存池单元测试：验证 alloc / free / 复用 / 池空
#include "memory_pool.h"
#include <cassert>
#include <cstdio>
#include <set>
#include <vector>

int main() {
    const size_t BLOCK = 128;   // 每块 128 字节
    const size_t COUNT = 10;    // 一共 10 块
    MemoryPool pool(BLOCK, COUNT);

    // ① 连续 alloc 10 次：应该拿到 10 个不同的块
    std::vector<void*> blocks;
    std::set<void*> seen;
    for (size_t i = 0; i < COUNT; i++) {
        void* p = pool.alloc();
        assert(p != nullptr);            // 池没空，必须有块
        assert(seen.count(p) == 0);      // 不能拿到重复的块
        seen.insert(p);
        blocks.push_back(p);
    }

    // ② 池空了，再 alloc 应该返回 nullptr
    assert(pool.alloc() == nullptr);

    // ③ 全部归还，再 alloc 10 次应该全都能拿回来（复用）
    for (void* p : blocks) pool.free(p);
    for (size_t i = 0; i < COUNT; i++) {
        void* p = pool.alloc();
        assert(p != nullptr);            // 归还后必须能再次拿到
    }
    assert(pool.alloc() == nullptr);     // 又空了

    printf("MemoryPool all success\n");
    return 0;
}
