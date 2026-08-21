import os
import socket
import struct
import subprocess
import time
import signal
import asyncio

# 解决运行环境不同的问题
# 开发者可通过环境变量覆盖默认值
# example : `NF_HOST=192.168.159.128 python3 ...`
HOST = os.getenv("NF_HOST", "127.0.0.1")
PORT = int(os.getenv("NF_PORT", "8888"))
TIMEOUT = float(os.getenv("NF_TIMEOUT", "3"))

# ========================================
# 协议
# ========================================

# 协议：[ 4 字节消息长度 ][ payload ]
_HEADER = struct.Struct("!I")


def build_msg(payload: bytes) -> bytes:
    """按协议封装一条消息。"""
    return _HEADER.pack(len(payload)) + payload


def _parser_header(header: bytes) -> int:
    """解析协议头，返回 payload 长度。"""
    (size,) = _HEADER.unpack(header)
    return size


# ========================================
# 同步 socket
# ========================================


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    """从 TCP 连接中读取恰好 size 字节。"""
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Connection Closed")
        data.extend(chunk)
    return bytes(data)


def recv_msg(sock: socket.socket) -> bytes:
    """接收一条完整协议消息并返回 payload。"""
    header = _recv_exact(sock, _HEADER.size)
    size = _parser_header(header)
    return _recv_exact(sock, size)


def send_msg(sock: socket.socket, payload: bytes) -> None:
    """发送一条完整协议消息。"""
    sock.sendall(build_msg(payload))


def roundtrip(sock: socket.socket, payload: bytes) -> bytes:
    """发送一条消息，并接收对应回复。"""
    send_msg(sock, payload)
    return recv_msg(sock)


# ========================================
# 异步 socket
# ========================================


async def async_recv_msg(reader: asyncio.StreamReader) -> bytes:
    """异步接收一条完整协议消息。"""
    header = await reader.readexactly(_HEADER.size)
    size = _parser_header(header)
    return await reader.readexactly(size)


async def async_send_msg(writer: asyncio.StreamWriter, payload: bytes) -> None:
    """异步发送一条完整协议消息。"""
    writer.write(build_msg(payload))
    await writer.drain()


async def async_roundtrip(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, payload: bytes
) -> bytes:
    """异步发送一条消息并等待回复。"""
    await async_send_msg(writer, payload)
    return await async_recv_msg(reader)


# ========================================
# 服务器生命周期控制
# ========================================


def connect(timeout=None) -> socket.socket:
    """创建到测试服务器的 TCP 连接。"""
    if timeout is None:
        timeout = TIMEOUT
    return socket.create_connection((HOST, PORT), timeout=timeout)


def start_server(binary: str, *args: str) -> subprocess.Popen:
    """启动服务器并等待其开始监听"""
    process = subprocess.Popen([binary, *args])
    wait_for_server(process)
    return process


def wait_for_server(process: subprocess.Popen, timeout: float = 5.0):
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


def stop_server(process: subprocess.Popen, timeout: float = 3.0):
    """关闭服务器，超时后强制结束"""
    if process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
