import signal
import socket
import subprocess
import time
from contextlib import contextmanager
from enum import Enum

from support.config import HOST, PORT, TIMEOUT


# ========================================
# 服务器生命周期控制
# ========================================


class ServerLogMode(Enum):
    INHERIT = "inherit"
    DISCARD = "discard"


def connect(timeout: float = TIMEOUT) -> socket.socket:
    """创建到测试服务器的 TCP 连接。"""
    return socket.create_connection((HOST, PORT), timeout=timeout)


def _start_server(binary: str, *args: str, log_mode: ServerLogMode) -> subprocess.Popen:
    """启动服务器并等待其开始监听"""
    if log_mode is ServerLogMode.DISCARD:
        return subprocess.Popen(
            [binary, *args], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
    return subprocess.Popen([binary, *args])


def _wait_for_server(process: subprocess.Popen, timeout: float = 5.0):
    """等待服务器开始监听。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(f"server exited before becoming ready: {return_code=}")
        try:
            with socket.create_connection((HOST, PORT), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(
        f"server did not become ready at {HOST}:{PORT} within {timeout}s"
    )


def _stop_server(process: subprocess.Popen, timeout: float = 3.0):
    """关闭服务器，超时后强制结束"""
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@contextmanager
def running_server(
    binary: str,
    *args: str,
    log_mode: ServerLogMode = ServerLogMode.INHERIT,
):
    process = _start_server(binary, *args, log_mode=log_mode)
    try:
        _wait_for_server(process)
        yield process
    finally:
        _stop_server(process)
