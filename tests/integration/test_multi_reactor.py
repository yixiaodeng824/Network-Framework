# -*- coding: utf-8 -*-
"""主从 multi-reactor 的多连接与优雅退出集成测试。"""

from concurrent.futures import ThreadPoolExecutor
import os
import signal
import socket
import subprocess
import time

from tests.common import build_msg, recv_msg


HOST = "127.0.0.1"
SUB_COUNT = 2
CONNECTION_COUNT = SUB_COUNT + 4
SERVER_BINARY = os.environ["NF_SERVER_BINARY"]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return int(sock.getsockname()[1])


def _wait_for_server(
    process: subprocess.Popen, port: int, timeout: float = 5.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(f"server exited before becoming ready: {return_code=}")
        try:
            with socket.create_connection((HOST, port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server did not become ready on port {port}")


def _start_echo_server(sub_count: int = SUB_COUNT):
    port = _free_port()
    process = subprocess.Popen(
        [
            SERVER_BINARY,
            "-p",
            str(port),
            "-s",
            str(sub_count),
            "-m",
            "echo",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_server(process, port)
    except Exception:
        process.kill()
        process.wait()
        raise
    return process, port


def _stop_server(process: subprocess.Popen) -> int:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            return int(process.wait(timeout=5))
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
    return int(process.returncode)


def _echo_once(port: int, payload: bytes) -> bytes:
    with socket.create_connection((HOST, port), timeout=5) as sock:
        sock.sendall(build_msg(payload))
        return recv_msg(sock)


def test_multi_sub_connections_are_served() -> None:
    """连接数超过 sub 数量时，每条连接仍能被正确回显。"""
    process, port = _start_echo_server()
    payloads = [f"client-{index}".encode() for index in range(CONNECTION_COUNT)]
    try:
        with ThreadPoolExecutor(max_workers=CONNECTION_COUNT) as pool:
            received = list(pool.map(lambda payload: _echo_once(port, payload), payloads))
        assert received == payloads
    finally:
        assert _stop_server(process) == 0


def test_multi_sub_server_stops_cleanly() -> None:
    """多 sub 场景收到 SIGINT 后应关闭连接并干净退出。"""
    process, port = _start_echo_server(sub_count=3)
    clients = []
    try:
        for _ in range(6):
            clients.append(socket.create_connection((HOST, port), timeout=5))

        for index, sock in enumerate(clients):
            payload = f"active-client-{index}".encode()
            sock.sendall(build_msg(payload))
            assert recv_msg(sock) == payload

        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=5) == 0

        for sock in clients:
            try:
                assert sock.recv(1) == b""
            except ConnectionResetError:
                pass

        try:
            socket.create_connection((HOST, port), timeout=1)
        except OSError:
            pass
        else:
            raise AssertionError("server still accepts connections after SIGINT")
    finally:
        for sock in clients:
            sock.close()
        if process.poll() is None:
            _stop_server(process)
