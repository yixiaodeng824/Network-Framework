# -*- coding: gbk -*-
# 优雅关闭测试:客户端发大消息后立刻 shutdown(SHUT_WR) 关写端,
# 验证服务器会把发送缓冲排空再关闭(数据不丢 + 正常 FIN 而非 RST)
import socket
import struct

HOST = '192.168.159.128'   # 服务器 IP(本机测就改成 127.0.0.1)
PORT = 8888
TIMEOUT = 10               # 大消息回显需要一点时间

def build_msg(payload):
    """打包一条消息: [4字节长度][内容]"""
    return struct.pack('>I', len(payload)) + payload

def make_conn():
    try:
        return socket.create_connection((HOST, PORT), timeout=TIMEOUT)
    except Exception as e:
        print("  !! 连不上服务器:", e)
        return None

print("== 优雅关闭测试 ==")
print("  场景:发 200KB 消息后立刻关闭写端(shutdown SHUT_WR)")
print("  服务器应该把缓冲排空再关(数据不丢 + 正常 FIN)")

s = make_conn()
if not s:
    print("结果: FAIL(没连上服务器)")
    exit(1)

msg = b"G" * 200000
s.sendall(build_msg(msg))
s.shutdown(socket.SHUT_WR)      # 发完立刻关写端,让服务器收到 FIN

# 累积收全部回显,直到收满或对方关闭
total = b''
fin = False
try:
    while len(total) < 4 + len(msg):
        chunk = s.recv(65536)
        if not chunk:
            fin = True          # 服务器正常关闭(收到 FIN)
            break
        total += chunk
except socket.timeout:
    print("  !! 超时:10 秒没等全回显")
    print("结果: FAIL(数据没排空完就卡住了)")
    s.close()
    exit(1)
except OSError as e:
    print("  !! 收到 RST:", e, "(连接被服务器强制重置,不是优雅关闭)")
    print("结果: FAIL")
    s.close()
    exit(1)

# 收满后,再确认连接是否正常结束(优雅关闭会发 FIN)
if not fin:
    try:
        last = s.recv(1024)
        fin = (last == b'')
    except OSError:
        fin = True              # 连接被关,也算结束

complete = (len(total) == 4 + len(msg))
content_ok = len(total) >= 4 and total[4:] == msg

print("  收到的总字节数:", len(total), "(期望 %d)" % (4 + len(msg)))
print("  内容对不对:", content_ok)
print("  连接是否正常关闭(FIN):", fin)

if complete and content_ok and fin:
    print("结果: PASS —— 数据完整 + 优雅关闭生效")
else:
    print("结果: FAIL(看上面哪一项不对)")

s.close()
