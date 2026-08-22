from concurrent.futures import ThreadPoolExecutor

import pytest

from support.protocol import build_msg, recv_msg, roundtrip, send_msg

BATCH_COUNT = 100
LARGE_MESSAGE_SIZE = 200_000
CONCURRENT_CLIENTS = 8


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"hello",
        bytes(range(256)),
        b"\x00\xff\x00binary\x00payload\xff",
    ],
)
def test_roundtrip_payload(server, payload: bytes) -> None:
    """echo 应该原样返回任意 bytes，而不只是文本。"""
    with server.connect() as sock:
        assert roundtrip(sock, payload) == payload


def test_multiple_messages_in_single_write(server) -> None:
    """一次 write 中包含多帧时，服务器必须正确拆包并保持顺序。"""
    messages = [b"hello", b"world", b"third"]

    with server.connect() as sock:
        sock.sendall(b"".join(build_msg(msg) for msg in messages))
        received = [recv_msg(sock) for _ in messages]

    assert received == messages


def test_many_messages_preserve_order(server) -> None:
    """连续发送多条消息后，响应顺序不能错乱。"""
    messages = [f"ping-{i}".encode() for i in range(BATCH_COUNT)]

    with server.connect() as sock:
        for message in messages:
            send_msg(sock, message)

        received = [recv_msg(sock) for _ in messages]

    assert received == messages


def test_large_message(server) -> None:
    """消息远大于 server 单次 recv 缓冲区时仍应完整回显。"""
    payload = b"L" * LARGE_MESSAGE_SIZE

    with server.connect() as sock:
        assert roundtrip(sock, payload) == payload


def test_concurrent_clients(server) -> None:
    """多个连接并发收发时，各连接的数据不能串线。"""

    def echo_client(index: int) -> bytes:
        payload = f"client-{index}:".encode() + bytes([index]) * (1024 + index * 17)
        with server.connect() as sock:
            return roundtrip(sock, payload)

    with ThreadPoolExecutor(max_workers=CONCURRENT_CLIENTS) as pool:
        actual = list(pool.map(echo_client, range(CONCURRENT_CLIENTS)))

    expected = [
        f"client-{i}:".encode() + bytes([i]) * (1024 + i * 17)
        for i in range(CONCURRENT_CLIENTS)
    ]
    assert actual == expected
