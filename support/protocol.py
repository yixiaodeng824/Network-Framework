import asyncio
import socket
import struct

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
