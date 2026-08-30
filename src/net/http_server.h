#pragma once
#include <map>
#include <string>
#include <functional>
#include <string_view>

struct HttpRequest
{
    std::string method;
    std::string path;
    std::map<std::string, std::string> query;
    std::map<std::string, std::string> headers;
    std::string body;
};

static const char *reason_phrase(int status)
{
    switch (status)
    {
    case 200:
        return "OK";
    case 201:
        return "Created";
    case 400:
        return "Bad Request";
    case 404:
        return "Not Found";
    case 500:
        return "Internal Server Error";
    default:
        return "OK";
    }
}

struct HttpResponse
{
    int status = 200;//状态码
    std::string body;
    std::map<std::string, std::string> headers;
    void json(const std::string &s);
    std::string ToString() const;//把response转换成string类型可以直接发出去
};
//路由
class HttpRouter
{
public:
    using Handler = std::function<void(const HttpRequest &, HttpResponse &)>;
    void get(const std::string &path, Handler h);
    void post(const std::string &path, Handler h);
    void put(const std::string &path, Handler h);
    void del(const std::string &path, Handler h);
    bool route(const HttpRequest &req, HttpResponse &resp); // 分发, 404 兜底
private:
    std::map<std::string, std::map<std::string, Handler>> routes_; // method → path → handler
};

bool ParseHttpRequest(std::string_view raw, HttpRequest &req);
