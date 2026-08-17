# -*- coding: gbk -*-
# 心跳测试:验证服务器会踢掉"静默超时"的连接,保留"持续活跃"的连接
# 注意:服务器要用 -h 5 启动(5 秒超时),例如 ./server.out -p 8888 -h 5
import socket
import struct
import time

HOST = '192.168.159.128'   # 服务器 IP(本机测就改成 127.0.0.1)
PORT = 8888
TIMEOUT = 3

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
        return None

def make_conn():
    try:
        return socket.create_connection((HOST, PORT), timeout=TIMEOUT)
    except Exception as e:
        print("  !! 连不上服务器:", e)
        return None

print("== 心跳测试(需要服务器 ./server.out -h 5,5 秒超时) ==")

# --- 客户端 A:连上后不说话,应该被踢 ---
print("  [A] 静默连接:连上不说话,等 7 秒(>5s 超时)...")
a = make_conn()
kicked = False
if a:
    time.sleep(7)
    try:
        data = a.recv(1024)
        kicked = (data == b'')        # 收到 FIN = 被服务器关闭
    except OSError:
        kicked = True                 # 收到 RST 也算被踢
    a.close()
    print("  静默连接 7 秒后是否被踢:", kicked)
else:
    print("  跳过(A 没连上)")

# --- 客户端 B:持续发消息,不应该被踢 ---
print("  [B] 活跃连接:每 2 秒发一条,持续 12 秒(>5s 超时)...")
b = make_conn()
alive = True
if b:
    for i in range(6):                # 6 * 2s = 12 秒
        time.sleep(2)
        try:
            b.sendall(build_msg(("ping-%d" % i).encode()))
        except OSError:
            alive = False             # 发送失败 = 连接被关了
            break
        got = recv_one(b)
        if got != ("ping-%d" % i).encode():
            alive = False
            break
    b.close()
    print("  活跃连接 12 秒内是否没被踢:", alive)
else:
    print("  跳过(B 没连上)")

if kicked and alive:
    print("结果: PASS —— 静默的被踢、活跃的保留")
else:
    print("结果: FAIL(看上面哪项不对)")
