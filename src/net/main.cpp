#include "epoll_server.h"
#include "main_reactor.h"
#include "io_uring_server.h"
#include "Logger.h"
#include <string_view>
#include <cstdlib>
#include <cstring>
#include <string>
#include "http_server.h"
using namespace std;

int main(int argc,char* argv[]) {
	int port = 8888;
	int threadnum = 0;
	int heartbeats = 60;
	int sub_count = 2;    // sub 数量，默认 2，可用 -s 指定
	string mode = "http"; // http=HTTP 200 demo / echo=回显 / io_uring，用 -m 切换
	bool affinity = false;   // -a 绑核（每核独立，推荐压测时开启）
    size_t max_conn = 0;
    int handshake_timeout = 10;
    size_t max_buffer_size = 0;//最大连接数
    for (int i = 1; i < argc; i++) {
		if ((strcmp(argv[i], "-p") == 0 || (strcmp(argv[i], "--port") == 0) )){
			if (i+1 >= argc) {
				LOG_DEBUG("no port");
				break;
			}
			else {
				port = atoi(argv[i + 1]);
			}
		}
		if ((strcmp(argv[i], "-t") == 0 || (strcmp(argv[i], "--threads") == 0))) {
			if (i+1 >= argc) {
				LOG_DEBUG("no threadnum");
				break;
			}
			else {
				threadnum = atoi(argv[i + 1]);
			}
		}
		if ((strcmp(argv[i], "-h") == 0 || (strcmp(argv[i], "--heartbeats") == 0))) {
			if (i + 1 >= argc) {
				LOG_DEBUG("no heartbeats");
				break;
			}
			else {
				heartbeats = atoi(argv[i + 1]);
			}
		}
		if ((strcmp(argv[i], "-s") == 0 || (strcmp(argv[i], "--subs") == 0))) {
			if (i + 1 >= argc) {
				LOG_DEBUG("no sub_count");
				break;
			}
			else {
				sub_count = atoi(argv[i + 1]);
			}
		}
		if (strcmp(argv[i], "-a") == 0 || strcmp(argv[i], "--affinity") == 0) {
			affinity = true;
		}
		if ((strcmp(argv[i], "-m") == 0 || (strcmp(argv[i], "--mode") == 0))) {
			if (i + 1 >= argc) {
				LOG_DEBUG("no mode");
				break;
			}
			else {
				mode = argv[i + 1];
			}
		}
        if ((strcmp(argv[i], "-max") == 0))
        {
            if (i + 1 >= argc)
            {
                LOG_DEBUG("no max connection");
                break;
            }
            else
            {
                max_conn = atoi(argv[i + 1]);
            }
        }
        if (strcmp(argv[i], "--handshake-timeout") == 0) {
            if (i + 1 >= argc) {
                LOG_DEBUG("no handshake_timeout");
                break;
            } else {
                handshake_timeout = atoi(argv[i + 1]);
            }
        }
        if (strcmp(argv[i], "--max-buffer") == 0) {
            if (i + 1 >= argc) {
                LOG_DEBUG("no max_buffer");
                break;
            } else {
                max_buffer_size = atoi(argv[i + 1]);
            }
        }
    }
	if (mode == "io_uring") {
		IoUringServer srv(port, threadnum, heartbeats, affinity ? 0 : -1);
		srv.start();
		return 0;
	}
	if (sub_count <= 0) sub_count = 2;

    HttpRouter router;
    router.get("/", [](const HttpRequest &, HttpResponse &resp)
               { resp.json(R"({"hello":"world"})"); });
    router.get("/api/messages", [](const HttpRequest &, HttpResponse &resp)
               { resp.json(R"({"messages":[]})"); });
    
    ThreadPool tp(threadnum);
    MainReactor main(port, tp, heartbeats, handshake_timeout, sub_count, affinity,max_conn,max_buffer_size);
    if (mode == "http") {
        main.setMessageHandler([&router](EpollServer &server, ConnectionId id, std::string_view msg)
                               {
                                   HttpRequest req;
                                   HttpResponse resp;
                                   ParseHttpRequest(msg, req);            // ① 解析请求
                                   router.route(req, resp);               // ② 路由分发(调用业务 handler)
                                   server.sendToRaw(id, resp.ToString()); // ③ 发响应
                               });
    } else {
        main.setMessageHandler([](EpollServer &server, ConnectionId id, std::string_view msg) {
            server.sendTo(id, msg); // 回显
        });
    }
    main.start();
    return 0;
}
