# -*- coding: gbk -*-
import time
from concurrent.futures import ThreadPoolExecutor

from tests.common import connect, roundtrip, build_msg, recv_msg, send_msg

# ==== 测试1: 单条消息 ====
def test_single_message() -> None:
    with connect() as sock:
        assert roundtrip(sock, b'hello') == b'hello'

# ==== 测试2: 粘包(一次发两条) ====
def test_multiple_messages() -> None:
    messages = [b'hello', b'world']
    with connect() as sock:
        sock.sendall(b''.join(build_msg(msg) for msg in messages))    
        received = [recv_msg(sock) for _ in messages]
    assert received == messages

# ==== 测试3: 半包/大消息(3000字节,分两次发) ====
HALF_PACKET_SIZE = 3000
HALF_PACKET_SPLIT = 1024
HALF_PACKET_DELAY = 0.3
def test_partial_message() -> None:
    payload = b'A' * HALF_PACKET_SIZE
    packet = build_msg(payload)
    with connect() as sock:
        sock.sendall(packet[:HALF_PACKET_SPLIT])
        time.sleep(HALF_PACKET_DELAY)
        sock.sendall(packet[HALF_PACKET_SPLIT:])
        assert recv_msg(sock) == payload 

# ==== 测试4: 3个用户同时发消息 ====
def echo_client(payload: bytes) -> bytes:
    with connect() as sock:
        return roundtrip(sock, payload)

def test_concurrent_clients() -> None:
    payloads = [
        b'A' * 100,
        b'B' * 200,
        b'C' * 50,
    ]
    with ThreadPoolExecutor(max_workers=len(payloads)) as pool:
        received = list(pool.map(echo_client, payloads))
    assert received == payloads

# ==== 测试5: 发送缓冲——连续多条合并回显 ====
# 连续拼接多个完整协议帧后一次写入 TCP，
# 验证服务器能够正确拆分多条消息，并按原顺序完整回显。
BATCH_COUNT = 10
def test_batch_messages() -> None:
    messages = [f'batch-{i}'.encode() * 20 for i in range(BATCH_COUNT)]
    with connect() as sock:
        sock.sendall(b''.join(build_msg(msg) for msg in messages))
        received = [recv_msg(sock) for _ in messages]
    assert received == messages

# ==== 测试6: 大消息回显(200KB,远超 recv 的 1024 缓冲) ====
# 服务器要多次 recv 拼进 recv_buffer_,拆包后拼进 send_buffer_ 再发回
LARGE_MESSAGE_SIZE = 200_000
def test_large_message() -> None:
    payload = b'B' * LARGE_MESSAGE_SIZE
    with connect() as sock:
        assert roundtrip(sock, payload) == payload

# ==== 测试7: 100条小消息连发,验证顺序 ====
ORDERED_MESSAGE_COUNT = 100
def test_message_order() -> None:
    messages = [f'ping-{i}'.encode() for i in range(ORDERED_MESSAGE_COUNT)]
    with connect() as sock:
        for msg in messages:
            send_msg(sock, msg)
        received = [recv_msg(sock) for _ in messages]
    assert received == messages

