#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 聊天室广播验证:
#   3 个客户端连上服务器,各自发一条唯一消息。
#   广播规则是"除了说话的人,所有在线连接都收到"。
#   所以每个客户端应该收到"另外两个客户端"的消息,共 2 条,且不含自己的。
#
# 用法:
#   先把服务器跑起来(默认 8888 端口),然后:
#   python tests/chat_broadcast_test.py
#
# 协议: [4 字节网络序长度][消息体]

import socket
import struct
import threading
import time

HOST = "192.168.159.128"
PORT = 8888
CLIENTS = 3
TIMEOUT = 6  # 收消息的等待上限(秒)


def recv_msg(sock):
    """按协议读一条完整消息,返回消息体 bytes;连接断开返回 None"""
    hdr = b""
    while len(hdr) < 4:
        chunk = sock.recv(4 - len(hdr))
        if not chunk:
            return None
        hdr += chunk
    n = struct.unpack("!I", hdr)[0]
    body = b""
    while len(body) < n:
        chunk = sock.recv(n - len(body))
        if not chunk:
            return None
        body += chunk
    return body


def send_msg(sock, msg):
    """按协议发一条消息(自动加 4 字节长度头)"""
    sock.sendall(struct.pack("!I", len(msg)) + msg)


def run_client(idx, ready, got):
    """一个客户端的完整行为:连上 → 等所有人就绪 → 发消息 → 收广播"""
    s = socket.create_connection((HOST, PORT))
    s.settimeout(TIMEOUT)
    ready.append(idx)
    # 等所有客户端都连上,保证广播时大家都在线,不漏人
    while len(ready) < CLIENTS:
        time.sleep(0.05)
    send_msg(s, ("msg-from-%d" % idx).encode())
    try:
        while True:
            m = recv_msg(s)
            if m is None:
                break
            got[idx].append(m)
    except socket.timeout:
        pass
    s.close()


def main():
    ready = []
    got = [[] for _ in range(CLIENTS)]
    threads = [
        threading.Thread(target=run_client, args=(i, ready, got))
        for i in range(CLIENTS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = True
    for i in range(CLIENTS):
        expected = sorted({"msg-from-%d" % j for j in range(CLIENTS) if j != i})
        actual = sorted([m.decode() for m in got[i]])
        status = "PASS" if actual == expected else "FAIL"
        if status == "FAIL":
            ok = False
        print("[%s] client-%d 收到 %d 条: %s\n      期望: %s"
              % (status, i, len(actual), actual, expected))

    print("\n" + ("ALL PASS - 聊天室广播正常" if ok else "SOME FAIL - 有问题!"))
    return 0 if ok else 1


if __name__ == "__main__":
    exit(main())
