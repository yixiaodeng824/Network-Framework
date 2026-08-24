# Network-Framework

一个 Linux 下的 C++17 网络服务器框架:**主从多 Reactor(epoll/io_uring)+ 批量 IO + 业务回调注入**。

回显、聊天室、HTTP……只需要在 main 里换一个回调函数,框架代码一行不改。

## 特性

- **主从多 Reactor**:主线程只 accept + 轮询分发,每个 sub-reactor 独立 epoll、无锁单 owner
- **边缘触发 ET**:去掉 EPOLLONESHOT,读循环到 EAGAIN,`epoll_ctl MOD` 只在写事件掩码变化时发生
- **批量 recv / 机会式批量 send**:一次事件循环尽量收满(64KB 上限),本批次拆出多条消息时积攒统一发送(N 次 send → 1 次)
- **epoll 批量 1024**:事件数组与 `epoll_wait` maxevents 同步提升
- **io_uring 模式**(`-m io_uring`):原始 syscall 实现 ring,accept/recv/send 合一,一次 `io_uring_enter` 提交/收割 N 个 IO
- **绑核**(`-a`):accept 主线程绑核 0,每个 sub 线程绑独立核(1..N),每核独立
- **4 字节长度头协议**:自动解决 TCP 粘包 / 半包(io_uring 模式复用同一套拆包)
- **业务回调注入**:换一个回调 = 换一个业务(`-m echo/http`)
- **心跳检测 / 优雅关闭 / 异步日志**:超时踢连接、排空缓冲再断连、日志不阻塞业务
- **多进程压测客户端**:`benchmarks/echo_mp.py`,突破单进程 asyncio 客户端瓶颈

## 架构总览

三层,记住三个词:**主线程分发 / sub-reactor 干活 / worker 池干重活**。

- **主线程(MainReactor)**:只做 accept,轮询把连接 fd 分发给某个 sub(`run_robin_ % sub_count_`),不碰数据。
- **sub-reactor(EpollServer × N)**:每个线程一个独立 epoll,独占自己的 `fd_list`(单 owner 无锁),内联执行 recv → 拆包 → 业务回调 → 发送。
- **worker 池(ThreadPool)**:预留的“重活分级”通道(`queueInLoop → eventfd → handleWakeup` 回投),回显这类轻任务默认内联在 sub 线程,不投递(投递开销大于收益)。

```
客户端 ──TCP──>  ┌───────────────────────────────────────────────┐
客户端 ──TCP──>  │  主线程 acceptLoop · epoll(1024)              │
客户端 ──TCP──>  │  accept → 轮询分发 fd(令牌)                   │
                 └───────────────────┬───────────────────────────┘
                                     │ queueInLoop 投递 fd
                 ┌───────────────────▼───────────────────────────┐
                 │ sub-reactor 线程 ×N(每核独立 epoll,ET)         │
                 │  epoll_wait → 批量 recv(到 EAGAIN/64KB)        │
                 │  → processBufferedData 拆包 → 业务回调         │
                 │  → send_buffer_ 积攒 → 本批次统一 flush 发送    │
                 └───────────────────┬───────────────────────────┘
                                     │ 重活回投(eventfd 门铃)
                 ┌───────────────────▼───────────────────────────┐
                 │ worker 池(ThreadPool,重活分级,回显不走)        │
                 └───────────────────────────────────────────────┘
```

关键分界:**主线程只分发连接令牌,不碰数据;sub-reactor 内联处理 IO + 轻业务,无锁;worker 池只接重活**。

## 三种 IO 模型

| 模型 | 启动参数 | 说明 |
|---|---|---|
| epoll LT + EPOLLONESHOT | 历史版本 | 每次事件后要 `epoll_ctl MOD` 重新装弹,MOD 是纯开销 |
| **epoll ET(默认)** | 无(默认 echo) | 边缘触发,读到 EAGAIN 不 MOD;`epoll_ctl MOD` 仅在 EPOLLOUT 挂/摘时发生 |
| **io_uring** | `-m io_uring` | 单 ring 提交/收割 recv/send/accept,一次 enter 批量 IO;当前为单线程版 |

## 通信协议

```
[4 字节长度(网络序 uint32)] [消息体]
```

TCP 是字节流,本身没有“消息”边界——一次 `recv` 可能只读到半条,也可能一次读到好几条。所以每条消息前面带一个长度头,接收方按长度“知道读到多少算一条完整消息”。拆包在 `Connection::processBufferedData()` 里做。

## 核心模块

| 模块 | 一句话职责 |
|---|---|
| `MainReactor` | 主线程:accept + 轮询分发 fd 到 sub,`-s` 指定 sub 数量 |
| `EpollServer` | sub-reactor:独立 epoll(ET)+ 批量 recv + 机会式批量 send + 心跳 |
| `IoUringServer` | io_uring 版单线程服务器:`-m io_uring`,复用 `Connection` 拆包 |
| `Connection` | 每个客户端的档案袋:收/发缓冲 + 状态 + 代际令牌;负责拆包、塞包、三态发包 |
| `ThreadPool` / `WorkThread` / `MessageQueue` | worker 池(重活分级),预留跨线程回投通道 |
| `Logger` | 异步日志,不阻塞业务;`logger.conf` 可配置级别 / 格式 / 输出目标 |

## 软件设计决策

> 每个“坑”背后都对应一个设计决策,这也是本项目最值得讲的部分。

1. **边缘触发 ET + 读到 EAGAIN** —— 去掉 EPOLLONESHOT 与每次事件的 MOD。
   *原因*:ONESHOT 每次事件后必须 `epoll_ctl MOD` 重新装弹,在高频小包下是纯 syscall 开销;ET 下读循环到 EAGAIN 即可,`epoll_ctl MOD` 只在 EPOLLOUT 挂/摘等掩码状态变化时发生。
2. **单 owner 无锁** —— 每个 sub 独占自己的 `fd_list`,主线程只通过 `queueInLoop` 移交 fd,没有共享锁。
   *原因*:连接只被一个线程碰,不需要 `client_mutex`;跨线程的只有 eventfd 门铃队列(有锁)。
3. **代际令牌 ConnectionId(fd + generation)** —— 任务都带令牌,执行前校验代际。
   *原因*:连接关闭后 fd 可能被内核复用,旧任务带旧代际,校验不过就丢弃,防止误操作新连接。
4. **批量 recv** —— 一次读事件循环 recv 到 EAGAIN 或 64KB(防饿死),再统一拆包。
   *原因*:减少 wakeup 次数,一次事件处理尽量多的数据;ET 下若没读到 EAGAIN 就停,会丢读边沿(64KB 上限时重新制造读边沿)。
5. **机会式批量 send** —— 回包先入发送缓冲,本批次拆出多条消息时积攒,批次结束一次 send 发出;单条消息直接发。
   *原因*:突发/流水场景 N 次 send → 1 次;一问一答场景不积攒,不回退延迟。
6. **绑核(-a)** —— accept 主线程绑核 0,每个 sub 线程绑独立核 1..N。
   *原因*:固定线程到核,减少跨核迁移与缓存抖动;注意绑核是“稳定器”不是“放大器”,QPS 翻倍靠加核并行(sub_count),不是绑核本身。
7. **心跳用时间驱动** —— 每 10 个 tick(约 1 秒)扫一遍所有连接,超时的踢掉。
8. **优雅关闭** —— `recv == 0` 时先排空发送缓冲再 `close`。
9. **日志丢旧保新** —— 日志队列满时丢最旧,新日志优先。

## 编译与运行

在 Linux 下安装好 cmake(用 `cmake --version` 验证)。项目根目录执行 `build.sh` 编译,产物是 `build/server`。

```bash
./build.sh
./run.sh                      # 默认 8888 · echo 模式 · 2 个 sub
./run.sh -p 9090              # 换端口
./run.sh -t 4                 # 线程池 worker 数(默认 0 = 自动)
./run.sh -s 8                 # sub-reactor 数量(每核一个)
./run.sh -m http              # HTTP demo(远端合并的 demo)
./run.sh -m io_uring          # io_uring 模式(单线程 ring)
./run.sh -a                   # 绑核:主线程→0,sub→1..N(真机推荐)
./run.sh -h 30                # 心跳超时(秒,默认 60)
```

## 压测

### 客户端

- `benchmarks/echo.py`:单进程 asyncio 客户端(100/500/1000 并发各 10s)。
- `benchmarks/echo_mp.py`:**多进程压测客户端**(推荐)。单进程客户端在 ~10 万 QPS 时先被压满,多进程可突破。

```bash
python3 -m benchmarks.echo_mp --procs 16 --clients 100,500,1000 --duration 10
```

- `bench_threads.sh`:压测 + 线程占用采样(每 0.5s 采样 `/proc/<pid>/task`,按角色归类),输出 `thread_usage.csv` / `thread_detail.csv`。

```bash
NF_SERVER_ARGS="-a -s 4" NF_SUBS=4 NF_BENCH_CMD="python3 -m benchmarks.echo_mp --procs 16 --clients 1000" bash bench_threads.sh
```

### 结果(回显 · 64B payload · 8~16 进程客户端)

| 版本/配置 | 100 并发 QPS | 500 并发 QPS | 1000 并发 QPS |
|---|---|---|---|
| 单进程客户端(旧基线) | ~10.3 万 | ~9.0 万 | ~7.7 万 |
| 多进程 8 进程 + epoll1024 | 47.1 万 | 44.8 万 | 45.9 万 |
| **ET 边缘触发(2 sub)** | **49.9 万** | **48.6 万** | 42.6 万 |
| ET · 4 sub(8 进程) | — | — | ~55 万 |
| ET · 4 sub · 16 进程 · 绑核 | — | — | **~68 万**(两轮 ±1%) |
| ET · 8 sub · 16 进程 | — | — | ~71 万(宿主机空闲窗口) |
| io_uring 单线程(16 进程) | 22.0 万 | 23.0 万 | 20.9 万 |

### 结论

- **QPS 提升靠加核并行**:sub_count 2→4→8,QPS 43万→55万→71万;绑核是稳定器(同窗口内两轮波动 ±1%),不是放大器。
- **ET 优于 LT+ONESHOT**:去掉每次事件的 MOD,100/500 并发创新高。
- **io_uring 单线程 < 多 sub epoll**:机制已验证(一次 enter 批量 IO),单线程跑不过多核;下一步做每核一个 ring。
- **VM 是共享宿主机**:绝对数字随宿主机负载漂移(同配置可差近一倍),可信基准要上独占物理机 + `-a` 绑核。
- **worker 池 0%**:回显轻任务内联在 sub 线程,投递开销大于收益;重活(大消息/加解密/广播)再走 `queueInLoop` 分级。

## 后续规划

- io_uring 每核一个 ring(多线程 uring)
- 真机 + 绑核 + 每核独立压测验证
- worker 池“重活分级”可配置阈值(消息大小/回调类型)
- 定时器(最小堆/时间轮)、更多 HTTP 支持