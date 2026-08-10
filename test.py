# -*- coding: gbk -*-
import socket
import struct
import time
import threading

HOST = '192.168.159.128'   # 虚拟机的 IP
PORT = 8888

def build_msg(payload):
    """打包一条消息: [4字节长度][内容]"""
    return struct.pack('>I', len(payload)) + payload

def recv_one(sock):
    """按长度头协议,收一条完整消息"""
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

# ==== 用例1:正常 ====
s = socket.create_connection((HOST, PORT))
print("== 用例1:正常 ==")
s.sendall(build_msg(b"hello"))
print("  回显:", recv_one(s))
s.close()

# ==== 用例2:粘包(一次发两条) ====
s = socket.create_connection((HOST, PORT))
print("== 用例2:粘包 ==")
s.sendall(build_msg(b"hello") + build_msg(b"world"))
print("  第1条:", recv_one(s))
print("  第2条:", recv_one(s))
s.close()

# ==== 用例3:半包/大消息(3000字节,分两次发) ====
s = socket.create_connection((HOST, PORT))
print("== 用例3:大消息半包 ==")
big = b"A" * 3000
s.sendall(build_msg(big)[:1024])   # 先发前 1024 字节
time.sleep(0.3)                    # 停 0.3 秒:让服务器先收到一半(半包)
s.sendall(build_msg(big)[1024:])   # 再发剩下的
got = recv_one(s)
print("  回显长度:", len(got), "| 内容对不对:", got == big)
s.close()

# ==== 用例4:3个用户同时发消息 ====
print("== 用例4:3个用户同时发 ==")
names = [b"A" * 100, b"B" * 200, b"C" * 50]   # 3个用户,各发不同的内容
socks = []
for i in range(3):
    sk = socket.create_connection((HOST, PORT))
    socks.append(sk)

def send_one(i):
    socks[i].sendall(build_msg(names[i]))

# 3 个线程同时发送(模拟 3 个用户同时发消息)
threads = [threading.Thread(target=send_one, args=(i,)) for i in range(3)]
for t in threads:
    t.start()
for t in threads:
    t.join()

# 各自收各自的回显,验证不串线
ok = True
for i in range(3):
    got = recv_one(socks[i])
    correct = (got == names[i])
    if not correct:
        ok = False
    got_len = len(got) if got else 0
    print("  用户%d: 发%d字节 收到%d字节 | 内容对不对: %s" % (i+1, len(names[i]), got_len, correct))
for sk in socks:
    sk.close()

print("  并发结果:", "全部正常,不串线!" if ok else "有串线,出问题了!")
