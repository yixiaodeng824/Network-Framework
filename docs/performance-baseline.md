# 性能基线

这份基线对应性能路线 issue #22，用来给后续优化提供可重复的对比参考。

## 测试条件

- 日期：2026-08-26（运行环境本地时间）
- 构建：CMake Release，C++17
- 服务器：`-m echo -s 2 -t 4`，未启用 CPU 绑核
- 客户端：`benchmarks.echo_mp`，8 个压测进程，64 字节消息
- 阶段：100、500、1000 连接，每阶段 10 秒
- 网络：本机回环接口

服务端指标使用 INFO 级别日志输出；运行前需要让 `logger.conf` 使用 `level = info`。客户端分位数使用 nearest-rank 计算，服务端分位数使用固定延迟桶估算，二者分别代表客户端端到端延迟和服务端处理延迟。

仓库已用 `benchmarks/echo.py` 和 `benchmarks/echo_mp.py` 替代旧的 `stress_test.py`；两个脚本现在都会输出并记录 p50/p95/p99/p999。

## 客户端基线

| 并发连接 | QPS | p50 (ms) | p95 (ms) | p99 (ms) | p999 (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 167236.3 | 0.40 | 1.78 | 4.66 | 9.95 |
| 500 | 163910.1 | 1.59 | 7.15 | 17.84 | 103.78 |
| 1000 | 171499.8 | 3.18 | 8.99 | 63.37 | 131.48 |

## 服务端汇总

三阶段结束并优雅退出后，`performance_metrics phase=final` 输出：

```text
qps=136163.7 requests=5763794
latency_avg_ms=0.006 p50_ms=0.005 p95_ms=0.020 p99_ms=0.020 p999_ms=0.500 latency_max_ms=157.278
active_connections=0 output_buffer_bytes=0
thread_pool_queue=0 thread_pool_wait_avg_ms=0.000 thread_pool_wait_max_ms=0.000
recv_eagain=5763794 send_eagain=0 accept_eagain=133 accept_failures=0
close_peer=1602 close_heartbeat=0 close_io=0 close_frame=0 close_send=0 close_overflow=0 close_shutdown=0 close_other=0
```

`thread_pool_queue` 和排队等待时间当前为 0，是因为 multi-reactor 的网络事件和回调由 sub-reactor owner loop 直接处理；线程池接口仍会记录后续 `post/submit` 任务的排队等待时间。`accept_eagain`、`recv_eagain` 是非阻塞 ET 模型下正常耗尽 socket 缓冲区的计数，不应直接视为失败。

## 复测命令

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel

# 确认 logger.conf 中为 level = info
./build/server -p 8888 -m echo -s 2 -t 4 > server.log 2>&1 &
server_pid=$!
NF_PORT=8888 python3 -m benchmarks.echo_mp \
  --procs 8 --clients 100,500,1000 --duration 10 --no-csv
kill -INT "$server_pid"
wait "$server_pid"

rg 'performance_metrics' server.log
```
