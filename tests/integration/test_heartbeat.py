import time

import pytest

from support.protocol import roundtrip
from support.server import ServerLauncher
from tests.integration.utils import assert_peer_closed

HEARTBEAT_TIMEOUT = 2
ACTIVE_TEST_DURATION = 4.0
ACTIVE_INTERVAL = 0.5


@pytest.fixture(scope="module")
def heartbeat_server(server_launcher: ServerLauncher):
    with server_launcher.running(heartbeat_timeout=HEARTBEAT_TIMEOUT) as server:
        yield server


def test_idle_connection_is_closed(heartbeat_server) -> None:
    """超过 heartbeat timeout 没有收到数据的连接应被 server 清理。"""
    with heartbeat_server.connect() as sock:
        assert_peer_closed(sock, timeout=HEARTBEAT_TIMEOUT + 2.5)


def test_active_connection_stays_alive(heartbeat_server) -> None:
    """持续有数据活动的连接不应被 heartbeat 误杀。"""
    deadline = time.monotonic() + ACTIVE_TEST_DURATION
    sequence = 0

    with heartbeat_server.connect() as sock:
        while time.monotonic() < deadline:
            payload = f"heartbeat-{sequence}".encode()
            assert roundtrip(sock, payload) == payload
            sequence += 1
            time.sleep(ACTIVE_INTERVAL)

        assert roundtrip(sock, b"final-check") == b"final-check"
