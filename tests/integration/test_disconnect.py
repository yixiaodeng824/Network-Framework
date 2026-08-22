import socket

from support.protocol import build_msg, recv_msg, roundtrip, send_msg
from tests.integration.utils import assert_peer_closed


def _assert_server_healthy(server) -> None:
    with server.connect() as sock:
        assert roundtrip(sock, b"health-check") == b"health-check"


def test_disconnect_mid_header_does_not_break_server(server) -> None:
    """客户端只发送部分 header 就断开时，不应影响其他连接。"""
    packet = build_msg(b"hello")

    sock = server.connect()
    try:
        sock.sendall(packet[:2])
    finally:
        sock.close()

    _assert_server_healthy(server)


def test_disconnect_mid_payload_does_not_break_server(server) -> None:
    """客户端在 body 未发完时断开，不应污染连接状态或拖垮 server。"""
    packet = build_msg(b"A" * 4096)

    sock = server.connect()
    try:
        sock.sendall(packet[:128])
    finally:
        sock.close()

    _assert_server_healthy(server)


def test_many_short_lived_connections_do_not_break_server(server) -> None:
    """频繁 connect/close 后 server 仍应能接受新连接。"""
    for _ in range(50):
        sock = server.connect()
        sock.close()

    _assert_server_healthy(server)


def test_half_close_flushes_response_before_fin(server) -> None:
    """
    客户端发送请求后 shutdown(SHUT_WR)：
    server 应先把已经产生的响应发完，再关闭连接。
    """
    payload = b"G" * 200_000

    with server.connect() as sock:
        send_msg(sock, payload)
        sock.shutdown(socket.SHUT_WR)

        assert recv_msg(sock) == payload
        assert_peer_closed(sock)
