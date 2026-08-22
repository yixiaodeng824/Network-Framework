import struct
import time

import pytest

from support.protocol import build_msg, recv_msg, roundtrip
from tests.integration.utils import assert_peer_closed

MAX_FRAME_SIZE = 4 * 1024 * 1024
FRAGMENT_DELAY = 0.05


@pytest.mark.parametrize("split", [1, 2, 3])
def test_fragmented_header(server, split: int) -> None:
    """4 字节长度头本身被拆开到达时，服务器必须等待完整 header。"""
    payload = b"fragmented-header"
    packet = build_msg(payload)

    with server.connect() as sock:
        sock.sendall(packet[:split])
        time.sleep(FRAGMENT_DELAY)
        sock.sendall(packet[split:])

        assert recv_msg(sock) == payload


@pytest.mark.parametrize("payload_bytes_before_pause", [1, 17, 1024])
def test_fragmented_payload(server, payload_bytes_before_pause: int) -> None:
    """header 已完整但 body 只有一部分时，服务器不能提前解析。"""
    payload = b"P" * 4096
    packet = build_msg(payload)
    split = 4 + payload_bytes_before_pause

    with server.connect() as sock:
        sock.sendall(packet[:split])
        time.sleep(FRAGMENT_DELAY)
        sock.sendall(packet[split:])

        assert recv_msg(sock) == payload


def test_complete_frame_followed_by_partial_frame(server) -> None:
    """buffer 中同时存在完整帧和下一帧残片时，状态必须正确保留。"""
    first = build_msg(b"first")
    second = build_msg(b"second")

    with server.connect() as sock:
        sock.sendall(first + second[:2])

        assert recv_msg(sock) == b"first"

        time.sleep(FRAGMENT_DELAY)
        sock.sendall(second[2:])

        assert recv_msg(sock) == b"second"


def test_oversized_frame_closes_only_bad_connection(server) -> None:
    """超出最大帧长度的客户端应被关闭，但 server 本身必须继续服务。"""
    oversized_length = MAX_FRAME_SIZE + 1

    with server.connect() as bad_sock:
        bad_sock.sendall(struct.pack("!I", oversized_length))
        assert_peer_closed(bad_sock)

    with server.connect() as good_sock:
        assert roundtrip(good_sock, b"server-still-alive") == b"server-still-alive"
