# -*- coding: gbk -*-
import socket
import struct
import time
import threading

HOST = '192.168.159.128'   # 服务器 IP(本机测就改成 127.0.0.1)
PORT = 8888
TIMEOUT = 3                # 每步最多等 3 秒,超时打诊断而不是死等

def build_msg(payload):
    """打包一条消息: [4字节长度][内容]"""
    return struct.pack('>I', len(payload)) + payload

def recv_one(sock):
    """按4字节头协议,读一条完整消息;超时/断开返回 None"""
    try:
        header = b''
        while len(header) < 4:
            chunk = sock.recv(4 - len(header))
            if not chunk:
                return None
            header += chunk
        length = struct.unpack('>I', header)[0]
        body = b''
        while len(body) < length:
            chunk = sock.recv(length - len(body))
            if not chunk:
                return None
            body += chunk
        return body
    except socket.timeout:
        return None   # 超时没等到完整回显

def make_conn():
    """建连接,超时返回 None"""
    try:
        return socket.create_connection((HOST, PORT), timeout=TIMEOUT)
    except Exception as e:
        print("  !! 连不上服务器:", e)
        return None

def miss(got, what):
    """回显没等到时,打印统一诊断"""
    print("  !! 超时没等到回显(%s)" % what)
    print("     -> 服务器收到了吗?看虚拟机 server.log 有没有 'new client fd='")
    print("     -> 收到了没回显?多半是编译的还是旧 server.out(旧代码不会回显)")

# ==== 测试1: 单条消息 ====
print("== 测试1: 单条 ==")
s = make_conn()
if s:
    s.sendall(build_msg(b"hello"))
    got = recv_one(s)
    if got is None:
        miss(got, "测试1 hello")
    else:
        print("  收到:", got)
    s.close()

# ==== 测试2: 粘包(一次发两条) ====
print("== 测试2: 粘包 ==")
s = make_conn()
if s:
    s.sendall(build_msg(b"hello") + build_msg(b"world"))
    a, b = recv_one(s), recv_one(s)
    if a is None or b is None:
        miss(None, "测试2 hello/world")
    else:
        print("  第1条:", a)
        print("  第2条:", b)
    s.close()

# ==== 测试3: 半包/大消息(3000字节,分两次发) ====
print("== 测试3: 大消息半包 ==")
s = make_conn()
if s:
    big = b"A" * 3000
    s.sendall(build_msg(big)[:1024])   # 先发前 1024 字节
    time.sleep(0.3)                    # 停一下,让服务器先收到前一半
    s.sendall(build_msg(big)[1024:])   # 再发剩下的
    got = recv_one(s)
    if got is None:
        miss(got, "测试3 大消息")
    else:
        print("  收到长度:", len(got), "| 内容对不对:", got == big)
    s.close()

# ==== 测试4: 3个用户同时发消息 ====
print("== 测试4: 3个用户同时发 ==")
names = [b"A" * 100, b"B" * 200, b"C" * 50]
socks = []
for i in range(3):
    sk = make_conn()
    if sk:
        socks.append(sk)

if len(socks) == 3:
    def send_one(i):
        socks[i].sendall(build_msg(names[i]))

    threads = [threading.Thread(target=send_one, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = True
    for i in range(3):
        got = recv_one(socks[i])
        correct = (got == names[i])
        if got is None:
            miss(got, "测试4 用户%d" % (i+1))
            correct = False
        if not correct:
            ok = False
        got_len = len(got) if got else 0
        print("  用户%d: 发%d字节 收到%d字节 | 内容对不对: %s" % (i+1, len(names[i]), got_len, correct))
    for sk in socks:
        sk.close()
    print("  测试4结果:", "全部正确!" if ok else "有错误!")

# ==== 测试5: 发送缓冲——连续多条合并回显 ====
# 一口气发 10 条(粘包)。服务器 processBufferedData 会把它们拼进 send_buffer_,
# sendReadyMessage 一次发回,验证"多条拼缓冲、合并发送"正确
print("== 测试5: 10条合并回显(发送缓冲) ==")
s = make_conn()
if s:
    msgs = [("batch-%d" % i).encode() * 20 for i in range(10)]
    for m in msgs:
        s.sendall(build_msg(m))
    ok = True
    for i in range(10):
        got = recv_one(s)
        if got is None or got != msgs[i]:
            ok = False
    print("  10条内容与顺序对不对:", ok)
    s.close()

# ==== 测试6: 大消息回显(200KB,远超 recv 的 1024 缓冲) ====
# 服务器要多次 recv 拼进 recv_buffer_,拆包后拼进 send_buffer_ 再发回
print("== 测试6: 200KB大消息回显 ==")
s = make_conn()
if s:
    big = b"B" * 200000
    s.sendall(build_msg(big))
    got = recv_one(s)
    if got is None:
        miss(got, "测试6 大消息")
    else:
        print("  收到长度:", len(got), "| 内容对不对:", got == big)
    s.close()

# ==== 测试7: 100条小消息连发,验证顺序 ====
print("== 测试7: 100条小消息连发 ==")
s = make_conn()
if s:
    for i in range(100):
        s.sendall(build_msg(("ping-%d" % i).encode()))
    ok = True
    for i in range(100):
        got = recv_one(s)
        if got is None or got != ("ping-%d" % i).encode():
            ok = False
    print("  100条顺序对不对:", ok)
    s.close()


# ==== 测试8: 恶意长度头(最大帧长限制) ====
# 发一个声称 2GB 的长度头,服务器应检测超限并断开连接
print("== 测试8: 恶意长度头被拦截 ==")
s = make_conn()
if s:
    s.sendall(struct.pack('>I', 0x7FFFFFFF))   # 声称 2GB > 4MB 上限
    try:
        data = s.recv(1)                        # 服务器应立即关闭,recv 返回空
        if data == b'':
            print("  服务器已断开连接(超长长度头被拦截) ok")
        else:
            print("  !! 服务器未断开,收到了数据:", data)
    except ConnectionResetError:
        print("  服务器已断开连接(连接被重置) ok")
    except socket.timeout:
        print("  !! 服务器 3 秒内没断开:长度头没被拦截(检查第3步部署了没)")
    s.close()

# ==== 测试9: 慢客户端触发高水位 ====
# 客户端故意不收回复,服务器回显堆积超过 1MB 水位线,应主动断开连接
print("== 测试9: 慢客户端高水位断开 ==")
s = make_conn()
if s:
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)   # 接收缓冲设到最小,让服务器尽快堆积
    except Exception:
        pass
    s.settimeout(10)                                  # 发送阶段放宽超时
    payload = b"y" * (200 * 1024)                     # 每条 200KB
    total_sent = 0
    try:
        for i in range(40):                           # 40 条 = 8MB,远超水位线
            s.sendall(build_msg(payload))
            total_sent += len(payload)
            time.sleep(0.02)                          # 给服务器 recv 一点时间,别堵住自己发送缓冲
    except (BrokenPipeError, ConnectionResetError, socket.timeout):
        pass                                          # 发送途中服务器已断,说明高水位生效
    print("  已发送 %.1f MB(不接收回复)" % (total_sent / 1024.0 / 1024.0))
    # 服务器应已把回显堆积超水位并断开;现在开始读,直到 EOF
    s.settimeout(8)
    closed = False
    try:
        while True:
            data = s.recv(65536)
            if data == b'':
                closed = True
                break
    except ConnectionResetError:
        closed = True
    except socket.timeout:
        closed = False                                # 8 秒还没断:高水位没生效
    if closed:
        print("  服务器已主动断开连接(高水位生效) ok")
    else:
        print("  !! 服务器 8 秒内未断开:高水位没生效?")
    s.close()
print()
print("== 说明 ==")
print("  测试5~7 验证的是'发送缓冲拼装 + 发送'的正确性。")
print("  阻塞 socket 下 EPOLLOUT 续发分支很难真正走到;要等以后非阻塞改造才触发。")
