import pytest

from support.protocol import roundtrip
from support.server import ServerLauncher
from tests.integration.utils import assert_peer_closed


@pytest.fixture
def fresh_server(server_launcher: ServerLauncher):
    """
    每个 lifecycle test 都拿到独立 server。
    这些测试会主动终止进程，因此不能复用 module-scope 默认 server。
    """
    with server_launcher.running() as server:
        yield server


def test_sigint_stops_server_and_closes_active_clients(fresh_server) -> None:
    """SIGINT 后 server 应退出，并关闭所有现存客户端连接。"""
    clients = [fresh_server.connect() for _ in range(3)]

    try:
        for i, sock in enumerate(clients):
            payload = f"before-stop-{i}".encode()
            assert roundtrip(sock, payload) == payload

        assert fresh_server.stop(timeout=3.0) == 0

        for sock in clients:
            assert_peer_closed(sock)

        with pytest.raises(OSError):
            fresh_server.connect(timeout=0.3)
    finally:
        for sock in clients:
            sock.close()
