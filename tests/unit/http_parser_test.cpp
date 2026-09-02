// ParseHttpRequest + HttpResponse::ToString + HttpRouter 单元测试
#include "http_server.h"
#include <cassert>
#include <cstdio>
#include <string>

int main() {
    // ===== ① ParseHttpRequest =====
    std::string raw =
        "POST /api/messages?page=1&size=10 HTTP/1.1\r\n"
        "Host: localhost:8888\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 5\r\n"
        "\r\n"
        "hello";

    HttpRequest req;
    assert(ParseHttpRequest(raw, req));
    assert(req.method == "POST");
    assert(req.path == "/api/messages");
    assert(req.query["page"] == "1");
    assert(req.query["size"] == "10");
    assert(req.headers["Host"] == "localhost:8888");
    assert(req.headers["Content-Type"] == "application/json");
    assert(req.body == "hello");

    // ===== ② HttpResponse::ToString =====
    HttpResponse resp;
    resp.status = 201;
    resp.json(R"({"ok":true})");
    std::string out = resp.ToString();
    assert(out.find("HTTP/1.1 201 Created") == 0);
    assert(out.find("Content-Type: application/json") != std::string::npos);
    assert(out.find("Content-Length: 11") != std::string::npos);
    assert(out.find("\r\n\r\n{\"ok\":true}") != std::string::npos);

    // ===== ③ HttpRouter 路由分发 =====
    HttpRouter router;
    bool get_called = false, post_called = false;
    router.get("/api/messages", [&](const HttpRequest&, HttpResponse& r) {
        get_called = true;
        r.json(R"({"messages":[]})");
    });
    router.post("/api/messages", [&](const HttpRequest&, HttpResponse& r) {
        post_called = true;
        r.status = 201;
        r.json(R"({"ok":true})");
    });

    // GET 分发 → 200 + get_called
    HttpRequest get_req;
    get_req.method = "GET";
    get_req.path = "/api/messages";
    HttpResponse resp1;
    assert(router.route(get_req, resp1));
    assert(get_called);
    assert(resp1.status == 200);

    // POST 分发 → 201 + post_called
    HttpRequest post_req;
    post_req.method = "POST";
    post_req.path = "/api/messages";
    HttpResponse resp2;
    assert(router.route(post_req, resp2));
    assert(post_called);
    assert(resp2.status == 201);

    // 路径没注册 → 404
    HttpRequest unknown_req;
    unknown_req.method = "GET";
    unknown_req.path = "/unknown";
    HttpResponse resp3;
    assert(!router.route(unknown_req, resp3));
    assert(resp3.status == 404);

    // 方法没注册(PUT) → 404
    HttpRequest put_req;
    put_req.method = "PUT";
    put_req.path = "/api/messages";
    HttpResponse resp4;
    assert(!router.route(put_req, resp4));
    assert(resp4.status == 404);

    printf("HTTP 解析 + 响应组装 + 路由分发 测试全部通过\n");
    return 0;
}
