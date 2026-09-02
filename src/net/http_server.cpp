#include "http_server.h"
#include <cstdlib>
using namespace std;
bool ParseHttpRequest(string_view raw, HttpRequest &req){
    //截取出第一行请求行
    size_t line_end = raw.find("\r\n");
    if(line_end==string::npos)
        return false;
    string_view request_line = raw.substr(0, line_end);

    size_t pos1 = request_line.find(' ');
    req.method = string(request_line.substr(0, pos1));

    size_t pos2 = request_line.rfind(' ');
    string_view tar = request_line.substr(pos1 + 1, pos2 - pos1 - 1);
    size_t pos3 = tar.find("?");
    req.path = string(tar.substr(0, pos3));

    if(pos3!=string::npos){
        if (pos3 != string::npos)
        {
            string_view query = tar.substr(pos3 + 1);
            size_t pos = 0;
            while (pos < query.size())
            {
                size_t amp = query.find('&', pos); // 找本段的 & 分隔
                string kv = string(query.substr(pos, amp == string::npos ? string::npos : amp - pos));
                size_t eq = kv.find('=');
                if (eq != string::npos)
                {
                    req.query[kv.substr(0, eq)] = kv.substr(eq + 1); // key = 等号前, value = 等号后
                }
                if (amp == string::npos)
                    break;
                pos = amp + 1; // 跳到下一段
            }
        }
    }
    //header解析
    size_t start_pos = line_end + 2;                     // 跳过请求行的 \r\n
    size_t header_end = raw.find("\r\n\r\n", start_pos); // 空行位置(headers 结束)
    if (header_end == string::npos)
        return false;
    while(start_pos < header_end){
        size_t cur_end_pos = raw.find("\r\n", start_pos);
        size_t separate = raw.find(":", start_pos);
        if (separate >= cur_end_pos)
        { // 本行没冒号, 跳过这行
            start_pos = cur_end_pos + 2;
            continue;
        }
        string key = string(raw.substr(start_pos, separate - start_pos));
        string value = string(raw.substr(separate + 1, cur_end_pos - separate - 1));
        if (!value.empty() && value[0] == ' ')
            value = value.substr(1);
        req.headers.insert({key, value});
        start_pos = cur_end_pos + 2;
    }
    //body解析
    size_t body_start = header_end + 4;
    auto it = req.headers.find("Content-Length");
    if(it!=req.headers.end()){
        int len = atoi(it->second.c_str());
        if (body_start + len <= raw.size())
        {                                           // 数据够吗(防半包)
            req.body = raw.substr(body_start, len); // 取 len 字节
        }
    }
    return true;
}

void HttpResponse::json(const std::string &s)
{
    body = s;
    headers["Content-Type"] = "application/json";
}

std::string HttpResponse::ToString() const{
    string response{""};
    response += "HTTP/1.1 " + to_string(status) + " " + reason_phrase(status) + "\r\n";
    response += "Content-Length: " + to_string(body.size()) + "\r\n";
    for(auto& it:headers){
        response += it.first + ": " + it.second + "\r\n";
    }
    response += "\r\n";
    response += body;
    return response;
}

void HttpRouter::get(const std::string &path, Handler h)
{
    routes_["GET"][path] = std::move(h);
}
void HttpRouter::post(const std::string &path, Handler h)
{
    routes_["POST"][path] = std::move(h);
}
void HttpRouter::put(const std::string &path, Handler h)
{
    routes_["PUT"][path] = std::move(h);
}
void HttpRouter::del(const std::string &path, Handler h)
{
    routes_["DELETE"][path] = std::move(h);
}

bool HttpRouter::route(const HttpRequest &req, HttpResponse &resp){
    // 第一层: 按方法找
    auto it = routes_.find(req.method);
    if (it == routes_.end())
    {
        resp.status = 404;
        resp.json(R"({"error":"not found"})");
        return false;
    }
    // 第二层: 按路径找 (it->second 是内层 map)
    auto it2 = it->second.find(req.path);
    if (it2 == it->second.end())
    {
        resp.status = 404;
        resp.json(R"({"error":"not found"})");
        return false;
    }
    it2->second(req, resp); // 调用找到的 handler
    return true;
}