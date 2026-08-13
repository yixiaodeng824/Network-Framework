# -*- coding: gbk -*-
import socket
import struct
import time
import threading

HOST = '192.168.130.128'   # ������� IP
PORT = 8888

def build_msg(payload):
    """���һ����Ϣ: [4�ֽڳ���][����]"""
    return struct.pack('>I', len(payload)) + payload

def recv_one(sock):
    """������ͷЭ��,��һ��������Ϣ"""
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

# ==== ����1:���� ====
s = socket.create_connection((HOST, PORT))
print("== ����1:���� ==")
s.sendall(build_msg(b"hello"))
print("  ����:", recv_one(s))
s.close()

# ==== ����2:ճ��(һ�η�����) ====
s = socket.create_connection((HOST, PORT))
print("== ����2:ճ�� ==")
s.sendall(build_msg(b"hello") + build_msg(b"world"))
print("  ��1��:", recv_one(s))
print("  ��2��:", recv_one(s))
s.close()

# ==== ����3:���/����Ϣ(3000�ֽ�,�����η�) ====
s = socket.create_connection((HOST, PORT))
print("== ����3:����Ϣ��� ==")
big = b"A" * 3000
s.sendall(build_msg(big)[:1024])   # �ȷ�ǰ 1024 �ֽ�
time.sleep(0.3)                    # ͣ 0.3 ��:�÷��������յ�һ��(���)
s.sendall(build_msg(big)[1024:])   # �ٷ�ʣ�µ�
got = recv_one(s)
print("  ���Գ���:", len(got), "| ���ݶԲ���:", got == big)
s.close()

# ==== ����4:3���û�ͬʱ����Ϣ ====
print("== ����4:3���û�ͬʱ�� ==")
names = [b"A" * 100, b"B" * 200, b"C" * 50]   # 3���û�,������ͬ������
socks = []
for i in range(3):
    sk = socket.create_connection((HOST, PORT))
    socks.append(sk)

def send_one(i):
    socks[i].sendall(build_msg(names[i]))

# 3 ���߳�ͬʱ����(ģ�� 3 ���û�ͬʱ����Ϣ)
threads = [threading.Thread(target=send_one, args=(i,)) for i in range(3)]
for t in threads:
    t.start()
for t in threads:
    t.join()

# �����ո��ԵĻ���,��֤������
ok = True
for i in range(3):
    got = recv_one(socks[i])
    correct = (got == names[i])
    if not correct:
        ok = False
    got_len = len(got) if got else 0
    print("  �û�%d: ��%d�ֽ� �յ�%d�ֽ� | ���ݶԲ���: %s" % (i+1, len(names[i]), got_len, correct))
for sk in socks:
    sk.close()

print("  �������:", "ȫ������,������!" if ok else "�д���,��������!")
