# -*- coding: gbk -*-
"""HTTP demo 集成测试：验证服务器作为 Web 服务器的 HTTP 拆包逻辑。

覆盖场景：
1. GET 无 body —— 最常见的浏览器请求
2. POST 带 body —— 依赖 Content-Length 正确解析
3. POST 带 body 后同连接再发 GET —— 验证 body 被完整消费、不污染下一个请求
4. 恶意超大 Content-Length —— 应触发帧错误被服务器断开
"""
from tests.common import connect

# ========================================
# HTTP 响应读取辅助
# ========================================


def _recv_until(sock, marker: bytes) -> bytes:
    """一直读到缓冲区中出现 marker（含 marker），用于读响应头。"""
    data = bytearray()
    while marker not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("连接被服务器关闭，提前读到 EOF")
        data.extend(chunk)
    return bytes(data)


def recv_http_response(sock) -> bytes:
    """按 HTTP 协议读一个完整响应：响应头 + Content-Length 指定长度的 body。"""
    data = _recv_until(sock, b"\r\n\r\n")
    header_end = data.index(b"\r\n\r\n") + 4
    header = data[:header_end]

    # 解析 Content-Length（可能一部分 body 已经在 data 里了）
    content_len = 0
    for line in header.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            content_len = int(line.split(b":", 1)[1].strip())
            break

    body = data[header_end:]
    while len(body) < content_len:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("连接被服务器关闭，响应 body 不完整")
        body += chunk
    return header + body


# ========================================
# 测试用例
# ========================================

# ==== 用例1: GET 无 body ====
def test_get_no_body() -> None:
    """浏览器最常见的请求：GET 无 body，应返回 200 OK + "ok"。"""
    with connect() as sock:
        sock.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        resp = recv_http_response(sock)
        assert resp.startswith(b"HTTP/1.1 200 OK")
        assert resp.endswith(b"ok")


# ==== 用例2: POST 带 body ====
def test_post_with_body() -> None:
    """POST 带 body（Content-Length: 5），应完整解析并返回 200。"""
    with connect() as sock:
        post = (b"POST /submit HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Length: 5\r\n"
                b"\r\n"
                b"hello")
        sock.sendall(post)
        resp = recv_http_response(sock)
        assert resp.startswith(b"HTTP/1.1 200 OK")
        assert resp.endswith(b"ok")


# ==== 用例3: POST 带 body 后同连接再发 GET（关键用例）====
def test_post_then_get_same_connection() -> None:
    """POST 的 body 必须被完整消费，否则残留字节会污染下一个请求。

    修复前 Content-Length 解析失败（body 残留在缓冲区），
    下一个 GET 会跟残留字节拼在一起，拆包错乱甚至连接被关。
    修复后两个请求都应收到 200 OK。
    """
    with connect() as sock:
        # 第一个请求：POST 带 body
        post = (b"POST /submit HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Length: 5\r\n"
                b"\r\n"
                b"hello")
        sock.sendall(post)
        resp1 = recv_http_response(sock)
        assert resp1.startswith(b"HTTP/1.1 200 OK")

        # 第二个请求：同一个连接再发 GET，验证连接状态是干净的
        sock.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        resp2 = recv_http_response(sock)
        assert resp2.startswith(b"HTTP/1.1 200 OK")


# ==== 用例4: 恶意超大 Content-Length ====
def test_malicious_content_length() -> None:
    """恶意客户端声明巨大的 Content-Length（>4MB），应触发帧错误被服务器断开。"""
    with connect() as sock:
        evil = (b"POST / HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Length: 999999999\r\n"
                b"\r\n")
        sock.sendall(evil)
        # 服务器应主动断开：recv 读到空（FIN）或抛异常（RST）都算断开
        try:
            got = sock.recv(4096)
        except ConnectionResetError:
            return  # RST 也算断开
        assert got == b"", f"服务器应断开连接，却收到了数据: {got!r}"

# ========================================
# /health liveness probe (issue #47)
# ========================================
def test_health_probe() -> None:
    """GET /health liveness probe: 200 when the server is alive."""
    with connect() as sock:
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        resp = recv_http_response(sock)
        assert resp.startswith(b"HTTP/1.1 200 OK")
        assert b'"status":"ok"' in resp