# 外围任务清单（协作同学专用）

> 项目：Network-Framework（Linux C++17 网络框架，epoll 主从 Multi-Reactor）
> 分支：multi-reactor（当前主线）
> 协议：4 字节长度头（默认）＋ HTTP 识别（`-m http`）
> 端口默认 8888，参数：`-p` 端口 / `-t` 线程 / `-h` 心跳 / `-s` sub 数 / `-a` 绑核 / `-m` 模式
> 编译：`./build.sh`　测试：`ctest` 或 `python3 -m pytest tests/integration`

## 开始前必读

1. 拉代码：`git checkout multi-reactor`
2. 编译：`./build.sh`（产物 `build/server`）
3. 跑通测试：`python3 -m pytest tests/integration/test_http_demo.py -v`
4. 读 `README.md` 和 `docs/framework-qa.md` 了解架构

**铁律：外围任务只碰"外围"，不碰 `src/net/epoll_server.*`、`src/net/main_reactor.*`、`src/net/connection.*` 这些核心文件。** 核心由主开发负责，你只通过两个稳定接口对接：
- **命令行参数**（起服务器时传）
- **协议**（4 字节长度头：`[uint32 大端长度][payload]`；HTTP 识别按 `\r\n\r\n`）

## 任务分级

- **P0**：不做，服务器没法给人用
- **P2**：做完项目"看得见、摸得着"（简历展示层）
- **P3**：锦上添花

---

## P0-1　正式客户端库（Python 或 C++）

**目标**：给业务方一个 API 稳定的客户端，替代现在只有测试脚本的局面。

**要做的**：
- 连接管理：connect、断线自动重连
- 请求/响应匹配：每条消息带递增 ID，响应带回 ID（需要在协议里约定）
- 心跳保活：客户端定时发心跳，超时判定断线
- 线程安全：多线程可并发发消息

**接口边界**：只依赖 4 字节长度头协议 + 服务器地址/端口，不碰服务器代码。

**验收**：写一个 demo，客户端连上服务器后 `roundtrip` 1000 条消息全部匹配；断线后自动重连成功。

---

## P0-2　配置文件系统

**目标**：用 `server.conf` 替代命令行一堆 `-p -t -h -s -a -m`，方便部署。

**要做的**：
- 定义 `server.conf` 格式（参考 `logger.conf` 的风格：`key = value`）
- 启动时读配置，缺省用命令行参数，再缺省用默认值
- 支持：`port / threads / heartbeat / sub_count / affinity / mode / log_target / log_level`

**接口边界**：只改 `main.cpp` 的启动逻辑（参数解析），不碰核心类。

**验收**：只写一个 `server.conf` 就能启动服务器，且配置项全部生效；配置项缺失时能回退默认。

---

## P0-3　部署包

**目标**：服务器能被"一键跑起来"，能进 systemd、能进容器。

**要做的**：
- `Dockerfile`：基于 Ubuntu 22.04，编译 + 运行 `build/server`
- `docker-compose.yml`：映射端口、挂载 `server.conf` 和日志
- `systemd` 单元文件：`restart=always`、日志重定向、`ExecStart` 传参

**接口边界**：纯运维文件，不碰代码。

**验收**：`docker compose up` 后 `curl 127.0.0.1:8888` 能通；`systemctl start` 后服务器自启动。

---

## P0-4　运行监控指标

**目标**：运行时能观察服务器健康度（QPS、连接数、内存）。

**要做的**：
- 在服务器暴露一个 `GET /metrics`（走 HTTP 模式），返回文本：
  ```
  connections 123
  qps_total 4567890
  send_bytes 123456
  recv_bytes 654321
  ```
- 指标由服务器内部计数（在核心加几个 `std::atomic<uint64_t>` 计数器，或由主开发配合加）

**接口边界**：计数器的埋点由主开发在核心加，你负责 `/metrics` 的响应组装。

**验收**：压测时 `curl /metrics` 能看到 QPS 实时增长。

---

## P2-1　WebSocket 协议

**目标**：在长度头框架上加 WebSocket 支持（聊天室的正确通道）。

**要做的**：
- 基于 HTTP 识别（`-m http`），实现 WS 握手（`Sec-WebSocket-Accept` 计算）
- WS 帧解析：FIN/opcode/mask/长度，支持 text 帧
- 用核心的 `sendToRaw` 原样发 WS 帧（不加长度头）

**接口边界**：在业务回调层做，用 `sendToRaw`，不碰核心 IO。

**验收**：浏览器 `new WebSocket("ws://127.0.0.1:8888")` 能连上，双向收发消息。

---

## P2-2　聊天室 Web 前端

**目标**：一个 HTML 页面，多人实时聊天（项目的"被使用证据"）。

**要做的**：
- 简单 HTML+JS 页面：输入昵称、消息列表、发送框
- 连 `ws://服务器`，广播消息（服务器用 `broadcast` 转发）
- 一个 `index.html` 就够了，别搞工程化

**接口边界**：纯前端 + WebSocket，配 P2-1 一起做。

**验收**：开两个浏览器标签，互相能看到对方消息，不含自己。

---

## P2-3　HTTP 静态文件服务器

**目标**：服务器能返回磁盘文件（HTML/图片），当个真 Web 服务器。

**要做的**：
- `-m http` 下解析请求行路径（如 `GET /index.html`）
- 读文件返回，带 `Content-Type` 和 `Content-Length`
- **路径校验**：过滤 `..` / 绝对路径穿越，防止读任意文件

**接口边界**：业务回调层，用 `sendToRaw` 回 HTTP 响应。

**验收**：浏览器能打开服务器返回的 HTML 页面；访问 `../../etc/passwd` 返回 403。

---

## P3-1　GitHub Actions CI

**目标**：push 自动编译 + 跑集成测试。

**要做的**：`.github/workflows/ci.yml`，Ubuntu 环境，`cmake --build` + `ctest`。

**验收**：push 后 Actions 绿。

---

## P3-2　分布式压测

**目标**：多台机器压测，测真实网络下的表现（当前压测都是本机回环）。

**要做的**：改 `benchmarks/echo_mp.py` 支持 `NF_HOST` 指向远端服务器。

**验收**：两台机器，一台跑服务器、一台跑压测，能出 QPS 数据。

---

## 认领规则

- 按 P0 → P2 → P3 顺序做
- 每个任务开一个分支：`feature/<任务名>`，做完提 PR 到 `multi-reactor`
- 完成标准 = 验收那一栏能打勾，并在 PR 里贴验证输出
