# 网络编程函数笔记(Socket + epoll)

> 目标:一个 epoll 监听多个事件。
> 套路比喻:开一家餐厅。socket = 开店,bind = 贴门牌,listen = 挂"营业中",accept = 接待客人,recv/send = 听/回话,close = 关门。epoll = 雇个前台拿对讲机盯着所有桌子。

---

## 一、开店四件套

### 1. `socket()` — 造一个"号码牌"

```cpp
int socket(int domain, int type, int protocol);
```

- `domain` = `AF_INET`(用 IPv4 网络)
- `type` = `SOCK_STREAM`(流式、可靠,对应 TCP)
- `protocol` = `0`(让系统自动选)
- **返回**:一个新的文件描述符 `fd`(号码牌)。**失败返回 -1**

> 比喻:在墙上开一个窗口,领到一个号码牌,凭这个号码牌才能操作。

---

### 2. `htons()` — 字节序转换(最容易漏的坑!)

```cpp
uint16_t htons(uint16_t hostshort);
```

电脑存储数字分"大端/小端",而**网络规定统一用大端**。
`htons` = Host TO Network Short,把端口号从电脑的字节序转成网络的。

```cpp
addr.sin_port = htons(8888);   // 必须转,不转连不上!
```

---

### 3. `bind()` — 给号码牌贴上门牌号

```cpp
int bind(int fd, const struct sockaddr* addr, socklen_t len);
```

把 **IP + 端口** 绑到 socket 上。服务器必须 bind,别人才能找到你。

```cpp
sockaddr_in addr;
memset(&addr, 0, sizeof(addr));      // 先清零(重要!)
addr.sin_family = AF_INET;           // 协议族
addr.sin_port   = htons(8888);       // 端口(转字节序)
addr.sin_addr.s_addr = INADDR_ANY;   // 本机所有网卡都接受
bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
```

- `sockaddr_in` 是"专用结构体",`bind` 只认 `sockaddr*`,所以要**强转** `(sockaddr*)&addr`
- **返回**:0 成功,-1 失败
- 注意:端口被占用时 bind 会失败(用 `SO_REUSEADDR` 可快速重启复用)

> 比喻:在门上贴好"IP:8888"的门牌号,客人才能找到这家店。

---

### 4. `listen()` — 挂上"营业中"

```cpp
int listen(int fd, int backlog);
```

- `backlog` = 最多允许多少个连接在门口排队(如 5)
- 调用后**立即返回**,不阻塞。服务器开始"营业"

> 比喻:挂上营业牌,客人在门口排队等。

---

### 5. `accept()` — 接待一个客人

```cpp
int accept(int fd, struct sockaddr* addr, socklen_t* addrlen);
```

- 从排队队列里**取一个连接**,返回一个**全新的 fd**,专门服务这个客人
- **阻塞**:没客人时,程序停在这里等
- 原来的 `listen_fd` 继续留着接下一个客人
- 不关心对方地址可以传 `nullptr, nullptr`

> 比喻:来一个客人,服务员领他到一张新桌子(新 fd)。门(监听 fd)还开着接下一个。
> ⚠️ 每调用一次 accept **只接一个**客人——这就是旧版服务器"一次只能服务一个"的原因。

---

## 二、数据收发

### `recv()` — 听客人说话

```cpp
int recv(int fd, void* buf, size_t len, int flags);
```

- 从 `fd` 读数据存进 `buf`,最多读 `len` 字节
- `flags` 通常填 `0`
- **返回**(重点!):
  - `> 0`:实际读到的字节数
  - `0`:**对方断开了** → 该 `close(fd)` 了
  - `-1`:出错

### `send()` — 回话

```cpp
int send(int fd, const void* buf, size_t len, int flags);
```

- 把 `buf` 里 `len` 个字节发给对方
- **返回**:实际发送的字节数

---

## 三、收尾

### `close()` — 关门

```cpp
int close(int fd);
```

- 关闭一个 socket。服务器结束时**两个都要关**:客户端的 fd + 监听的 fd

---

## 四、epoll:一个前台管所有桌子

### 旧版的痛

旧版 `accept` 一次 + `recv` 循环 = **一个服务员只服务一桌**,其他客人干等,一个断线整个服务器退出。

### epoll 的思路

把所有 socket(监听 + 所有客户端)都**登记**给 epoll,然后 `epoll_wait` 一次性等所有,谁有动静处理谁。

### 1. `epoll_create1()` — 雇前台

```cpp
int epoll_create1(int flags);   // flags 填 0
```

- 返回 `epfd`(前台编号),之后所有 socket 都登记到这。

### 2. `epoll_ctl()` — 登记/取消登记

```cpp
int epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);
```

- `op` 三选一:
  - `EPOLL_CTL_ADD`:加进 epoll 盯着
  - `EPOLL_CTL_MOD`:修改关注的事件
  - `EPOLL_CTL_DEL`:移出 epoll(比如客户端断开)

`epoll_event` 是"登记表":

```cpp
epoll_event ev;
ev.events = EPOLLIN;   // 关注"可读"事件(有人发数据/有人来连接)
ev.data.fd = fd;       // 记住这个 socket 是谁
epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
```

### 3. `epoll_wait()` — 前台喊话

```cpp
int epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);
```

- `events`:一个数组,epoll 把"有事的"填进来
- `maxevents`:数组大小(一次最多处理几个)
- `timeout`:
  - `-1`:一直等,直到有事
  - `0`:不等,立即返回
  - `N`:最多等 N 毫秒
- **返回**:有事的个数 `n`,然后 `for (i = 0; i < n; i++)` 逐个处理

---

## 五、epoll 完整套路(背下来!)

```cpp
// 1. 开店:socket + bind + listen
// 2. 雇前台
int epfd = epoll_create1(0);

// 3. 让前台盯着门
epoll_event ev;
ev.events = EPOLLIN;
ev.data.fd = listen_fd;
epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);

epoll_event events[64];

while (true) {
    int n = epoll_wait(epfd, events, 64, -1);   // 前台喊话

    for (int i = 0; i < n; ++i) {
        int fd = events[i].data.fd;

        if (fd == listen_fd) {
            // 新客人 → accept,接进来,登记进 epoll
        } else {
            // 老客人发数据 → recv 回显;断开 → DEL + close
        }
    }
}
```

---

## 六、新旧对比

| | 旧 echo | epoll echo |
|---|---|---|
| 能服务几个客户端 | 1 个 | 很多个 |
| 其他客户端 | 排队干等 | 随时都能说话 |
| 一个客户端断开 | 服务器退出 | 不影响,继续服务 |
| 关键函数 | accept + recv 循环 | epoll_wait 循环 |
