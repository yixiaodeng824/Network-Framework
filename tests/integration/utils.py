import errno
import socket
import time

import pytest

_CLOSED_ERRNOS = {
    errno.ECONNRESET,
    errno.ECONNABORTED,
    errno.ENOTCONN,
    errno.EPIPE,
}


def assert_peer_closed(sock: socket.socket, timeout: float = 2.0) -> None:
    """等待对端关闭连接；FIN 或 RST 都视为连接已关闭。"""
    deadline = time.monotonic() + timeout
    previous_timeout = sock.gettimeout()

    try:
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            sock.settimeout(min(0.2, max(remaining, 0.01)))

            try:
                data = sock.recv(1)
            except TimeoutError:
                continue
            except (ConnectionResetError, BrokenPipeError):
                return
            except OSError as exc:
                if exc.errno in _CLOSED_ERRNOS:
                    return
                raise

            if data == b"":
                return

            pytest.fail(
                f"expected peer to close the connection, but received {data!r}",
                pytrace=False,
            )

        pytest.fail(
            f"peer did not close the connection within {timeout:.1f}s",
            pytrace=False,
        )
    finally:
        sock.settimeout(previous_timeout)
