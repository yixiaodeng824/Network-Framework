# Network-Framework

一个 Linux 下的 C++17 高性能网络框架:**主从多 Reactor(epoll/io_uring) + 批量 IO + 业务回调注册 + 内存池**。

做"回显、聊天、HTTP"都只需要在 main 里换一个回调函数,框架处理一切。

## 特性

- **主从多 Reactor**:主线程只 accept + 轮询分发,每个 sub-reactor 各自 epoll 且单 owner
- **边缘触发 ET**:去掉 EPOLLONESHOT,循环读到 EAGAIN,`epoll_ctl MOD` 只在写事件状态变化时调用
- **recvmmsg 批量收**:一次系统调用收最多 8 段 4KB(`RECVMMSG_BATCH`),事件层 + syscall 层都批量
- **批量式发送 send**:攒批 + 轮末统一 flush(N 次 send 合成 1 次)
- **内存池(MemoryPool + PoolAllocator)**:拆包消息从池分配(≤128B 走池,>128B new),每 sub 一个池无锁;`string_view` 隔离业务层
- **shared_ptr 连接保活**:`fd_list` 存 `shared_ptr<Connection>`,回调期连接不销毁,防悬空 + 防 fd 复用误操作
- **绑核(`-a`)**:accept 主线程绑核 0,每个 sub 线程绑核 1..N,每核独立
- **io_uring 模式**(`-m io_uring`):原始 syscall 实现 ring,accept/recv/send 合一,一次 `io_uring_enter` 提交/收割 N 个 IO
- **4 字节长度头协议**:自定义 TCP 粘包/半包(io_uring 模式复用同一套拆包)
- **业务回调注册**:一个回调 = 一种业务(`-m echo/http`)
- **心跳检测 / 优雅关闭 / 异步日志**:超时踢连接、排空缓冲再关、日志不阻塞业务
- **多进程压测客户端**:`benchmarks/echo_mp.py`,突破单进程 asyncio 客户端瓶颈

## 架构总览

记住,三句话:**主线程分发 / sub-reactor 干活 / worker 做重活**。

- **主线程(MainReactor)**:只 accept,轮询把 fd 分发到某个 sub(`run_robin_ % sub_count_`),不碰数据。
- **sub-reactor(EpollServer × N)**:每线程一个独立 epoll,独占自己的 `fd_list`(单 owner 无锁),循环执行 recv → 拆包 → 业务回调 → 发送。
- **worker 线程(ThreadPool)**:预留的"重活"通道(`queueInLoop → eventfd → handleWakeup` 投递),小消息默认在 sub 线程直接处理,投递是备选。

```
客户端 ──TCP──>  ┌──────────────────────────────────────────────────────┐
客户端 ──TCP──>  │  主线程 acceptLoop + epoll(1024)                     │
客户端 ──TCP──>  │  accept → 轮询分发 fd(不碰数据)                      │
                 └──────────────────────────────────────────────────────┘
                                     │ queueInLoop 投递 fd
                 ┌──────────────────────────────────────────────────────┐
                 │ sub-reactor 线程 ×N(每核独立 epoll,ET)               │
                 │  epoll_wait → recvmmsg 批量收(到 EAGAIN/64KB)        │
                 │  → processBufferedData 拆包(内存池) → 业务回调        │
                 │  → send_buffer_ 攒批 → 轮末统一 flush 发送             │
                 └──────────────────────────────────────────────────────┘
                                     │ 重活投递(eventfd 门铃)
                 ┌──────────────────────────────────────────────────────┐
                 │ worker 线程(ThreadPool,重活备用,默认不参与)           │
                 └──────────────────────────────────────────────────────┘
```

关键字:**主线程只分发不碰数据;sub-reactor 做 IO + 小业务,单 owner 无锁;worker 只做重活**。

## IO 模式

| 模式 | 启动方式 | 说明 |
|---|---|---|
| epoll LT + EPOLLONESHOT | 历史版本 | 每事件都要 `epoll_ctl MOD` 重新武装,MOD 是开销 |
| **epoll ET(默认)** | 默认(echo) | 边缘触发,读到 EAGAIN 才停 MOD;`epoll_ctl MOD` 只在 EPOLLOUT 挂/摘时调用 |
| **io_uring** | `-m io_uring` | 单 ring 提交/收割 recv/send/accept,一次 enter 管多个 IO;当前为单线程版 |

## 通信协议

```
[4 字节长度(网络序 uint32)] [消息体]
```

TCP 是字节流,没有"消息"边界——一个 `recv` 可能只收到一半,也可能一次收到好几条。每条消息前带 4 字节长度头,接收端就能知道"一条完整消息有多长"。拆包逻辑在 `Connection::processBufferedData()`。

## 核心模块

| 模块 | 一句话职责 |
|---|---|
| `MainReactor` | 主线程:accept + 轮询分发 fd 到 sub,`-s` 指定 sub 数量 |
| `EpollServer` | sub-reactor:独立 epoll(ET)+ recvmmsg 批量收 + 批量发送 + 心跳 |
| `IoUringServer` | io_uring 版单线程服务器:`-m io_uring`,复用 `Connection` 拆包 |
| `Connection` | 每个客户端的单元:收/发缓冲 + 状态 + 内存池指针;拆包/回包都在这 |
| `MemoryPool` | 内存池:固定 128B 块 + 空闲链表,alloc/free O(1),每 sub 一个,无锁 |
| `PoolAllocator` | 让 `std::string` 用内存池的自定义分配器:≤128B 走池,>128B new |
| `ThreadPool` / `WorkThread` / `MessageQueue` | worker 线程池(重活备用),`queueInLoop` 投递通道 |
| `Logger` | 异步日志,不阻塞业务;`logger.conf` 配置级别 / 格式 / 输出目标 |

## 关键设计决策

> 每一个"坑"背后都对应一个设计决策,这也是本项目最值得讲的部分。

1. **边缘触发 ET + 读到 EAGAIN** —— 去掉 EPOLLONESHOT 和每事件 MOD。
   *原因*:ONESHOT 每事件都要 `epoll_ctl MOD` 重新武装,高频小包是纯 syscall 开销;ET 下循环读到 EAGAIN 读空,`epoll_ctl MOD` 只在 EPOLLOUT 挂/摘(状态变化)时调用。
2. **recvmmsg 批量收** —— 一次系统调用收最多 8 段 4KB。
   *原因*:循环 recv 是"每段一次 syscall",recvmmsg 把 N 次 syscall 合成 1 次;ET 下没读到 EAGAIN 就停会丢事件,靠 `need_rearm_read` + MOD 补救。
3. **单 owner 无锁** —— 每个 sub 独占自己的 `fd_list`,主线程只通过 `queueInLoop` 移交 fd,没有共享锁。
   *原因*:连接只被一个线程碰,不需要 `client_mutex`;跨线程只有 eventfd 门铃(轻量)。
4. **shared_ptr 连接保活** —— `fd_list` 存 `shared_ptr<Connection>`,回调前持有拷贝。
   *原因*:回调(业务代码)可能关连接或让 fd 被内核复用,值存储会悬空崩溃;shared_ptr 让"谁在用谁持有",回调后重新 find + 指针比较确认连接没被换。
5. **ConnectionId(fd + generation) 代际校验** —— 任务携带令牌,执行前校验。
   *原因*:连接关闭后 fd 可能被内核复用给新连接,队列里的旧任务会误操作新连接;校验"连接还在 && 代际没变"。
6. **内存池(MemoryPool + PoolAllocator)** —— 拆包消息从池分配,`string_view` 隔离业务层。
   *原因*:消息分配高频(每包一次)、小(64B)、短命(回调完就扔),池化省 new/delete 和碎片;`string_view` 让业务层不持有内存、不依赖池实现。
7. **批量式发送 send** —— 回调只塞缓冲,攒批到轮末统一 flush,一次 send 发多条。
   *原因*:突发/流水场景 N 次 send → 1 次;单条消息直接发,避免延迟;`ww` 记录 EPOLLOUT 挂没挂,只在状态变化时 MOD。
8. **绑核(-a)** —— accept 主线程绑核 0,每个 sub 线程绑核 1..N。
   *原因*:固定线程调度,减少迁移与缓存抖动;注意是"稳定"不是"放大",QPS 随可用核数(sub_count)增长。
9. **心跳超时踢连接** —— 每 10 个 tick(约 1 秒)扫一遍连接,超时的统一关。
   *原因*:死连接是沉默的,只能靠定时巡检;收集后统一关,避免遍历中 erase 迭代器失效。
10. **优雅关闭** —— 收到 `recv == 0` 时,先排空发送缓冲再 `close`。
    *原因*:对端断开时可能还有没发完的回复,直接 close 会丢数据。
11. **日志不阻塞业务** —— 日志写入是异步的,不占用业务时间。
    *原因*:worker 写日志可能很慢(尤其磁盘);时间敏感业务里日志该让路。

## 编译与运行

在 Linux 下安装 cmake(用 `cmake --version` 验证)后,在项目根目录执行 `build.sh` 编译,产物为 `build/server`。

```bash
./build.sh
./run.sh                      # 默认 8888 端口 echo 模式 2 个 sub
./run.sh -p 9090              # 换端口
./run.sh -t 4                 # 线程池 worker 数(默认 0 = 自动)
./run.sh -s 8                 # sub-reactor 数量(每核一个,配合 -a)
./run.sh -m http              # HTTP demo(远端合并前 demo)
./run.sh -m io_uring          # io_uring 模式(单线程 ring)
./run.sh -a                   # 绑核:主线程→0,sub→1..N(压测推荐)
./run.sh -h 30                # 心跳超时(秒,默认 60)
```

## 压测

### 客户端

- `benchmarks/echo.py`:单进程 asyncio 客户端(100/500/1000 并发 × 10s)。
- `benchmarks/echo_mp.py`:**多进程压测客户端**(推荐),单进程客户端 ~10 万 QPS 时先被压满,多进程才能喂饱服务器。

```bash
python3 -m benchmarks.echo_mp --procs 16 --clients 100,500,1000 --duration 10
```

- `bench_threads.sh`:压测 + 线程占用采集(每 0.5s 读 `/proc/<pid>/task`),输出 `thread_usage.csv` / `thread_detail.csv`。

```bash
NF_SERVER_ARGS="-a -s 4" NF_SUBS=4 NF_BENCH_CMD="python3 -m benchmarks.echo_mp --procs 16 --clients 1000" bash bench_threads.sh
```

### 数据(回显 64B payload,8~16 个进程客户端)

| 版本/配置 | 100 并发 QPS | 500 并发 QPS | 1000 并发 QPS |
|---|---|---|---|
| 单进程客户端(可扩展性) | ~10.3 万 | ~9.0 万 | ~7.7 万 |
| 绑核 8 线程 + epoll1024 | 47.1 万 | 44.8 万 | 45.9 万 |
| **ET 边缘触发(2 sub)** | **49.9 万** | **48.6 万** | 42.6 万 |
| ET + 4 sub(8 线程) | — | — | ~55 万 |
| ET + 4 sub + 16 进程 + 绑核 | — | — | **~68 万**(抖动 ±1%) |
| ET + 8 sub + 16 进程 | — | — | ~71 万(虚拟核上测) |
| io_uring 单线程(16 进程) | 22.0 万 | 23.0 万 | 20.9 万 |
| **内存池 + recvmmsg + 绑核(8 sub)** | **32.2 万** | **76.2 万** | **81.4 万** |

### 结论

- **QPS 随可用核数增长**:sub_count 2/4/8,QPS 43万/55万/71万;抖动小(同配置 ±1%),不是放大镜。
- **ET 优于 LT+ONESHOT**:去掉每事件 MOD,100/500 并发下明显高。
- **内存池零损耗**:接入内存池后 1000 并发 81.4 万,比同学版(71 万)更高,池化没有拖慢性能。
- **io_uring 单线程 < 多 sub epoll**:原理验证(一次 enter 管多个 IO),单线程跑不满;下一步每线程一个 ring。
- **VM 是共享文件夹**:压测结果可能有漂移(同配置可差一倍),基准要绑 CPU + `-a` 绑核。
- **worker 线程 0%**:小消息默认在 sub 线程处理,投递是备选;重活(大消息/接入/广播)走 `queueInLoop` 分级。

## 后续规划

- io_uring 每线程一个 ring(多线程 uring)
- 内存池多档位(按消息大小分池)或 A/B 验证单池收益
- worker 承担"重活"分级任务(消息大小/回调耗时)
- 超时定时器(最小堆/时间轮)替代 HTTP 轮询
