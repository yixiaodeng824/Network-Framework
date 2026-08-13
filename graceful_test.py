# -*- coding: gbk -*-
# =====================================================
#  优雅退出验证脚本(复杂场景)
#
#  验证目标:有客户端连接着时,服务器按 Ctrl+C,
#            应优雅关闭所有连接并干净退出。
#
#  用法:
#    1) 虚拟机:  cd ~/projects/for_linux && ./server.out
#    2) 本机:    python graceful_test.py
#    3) 看到 ">>> 现在去虚拟机按 Ctrl+C" 后,切到虚拟机按 Ctrl+C
#    4) 回来看本脚本输出:
#       3 个客户端都应显示 [OK] 连接被关闭
#       最后一行:服务器已退出
# =====================================================
import socket
import struct
import time
import threading
import sys

HOST = '192.168.159.128'   # 虚拟机 IP(若脚本放在虚拟机上跑,改成 127.0.0.1)
PORT = 8888

def build_msg(payload):
    """组一条消息: [4字节长度][内容]"""
    return struct.pack('>I', len(payload)) + payload

def client(name, keep_sending):
    """一个模拟客户端
    keep_sending=True  :持续发消息并等回显(让服务器的 worker 一直有活干)
    keep_sending=False :连上就挂机,不吭声(考验连接名单能否被正确关闭)
    """
    try:
        s = socket.create_connection((HOST, PORT))
    except Exception as e:
        print("[%s] 连不上服务器: %s" % (name, e))
        return
    print("[%s] 已连接" % name)
    i = 0
    try:
        while True:
            if keep_sending:
                i += 1
                s.sendall(build_msg(("msg-%d" % i).encode()))
            data = s.recv(1024)
            if not data:
                print("[%s] [OK] 连接被服务器优雅关闭" % name)
                break
            time.sleep(0.1)
    except ConnectionResetError:
        print("[%s] [RST] 连接被重置(服务器 close 时缓冲里还有数据)" % name)
    except (BrokenPipeError, OSError) as e:
        print("[%s] [异常] %s" % (name, e))
    finally:
        s.close()
        print("[%s] 已断开" % name)

def main():
    # 先探测服务器是否在监听
    try:
        p = socket.create_connection((HOST, PORT), timeout=3)
        p.close()
        print("服务器正在监听,开始测试...")
    except Exception:
        print("连不上服务器!请先在虚拟机启动 ./server.out")
        sys.exit(1)

    threads = [
        threading.Thread(target=client, args=("C1-挂机", False)),
        threading.Thread(target=client, args=("C2-说话", True)),
        threading.Thread(target=client, args=("C3-挂机", False)),
    ]
    for t in threads:
        t.daemon = True
        t.start()

    print("\n>>> 3 个客户端已连上服务器")
    print(">>> 现在去虚拟机按 Ctrl+C\n")
    print(">>> (C2 会一直发消息并等回显,模拟服务器正忙的连接)\n")

    for t in threads:
        t.join()

    print("\n--- 确认服务器进程已退出 ---")
    try:
        s = socket.create_connection((HOST, PORT), timeout=3)
        print("??? 服务器还在监听,没退干净!")
        s.close()
    except Exception:
        print("[OK] 服务器已退出(再连被拒绝)")

if __name__ == '__main__':
    main()
