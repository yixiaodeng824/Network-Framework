#include "epoll_server.h"
#include "main_reactor.h"
#include "Logger.h"
#include <string_view>
#include <cstdlib>
#include <cstring>
using namespace std;

int main(int argc,char* argv[]) {
	int port = 8888;
	int threadnum = 0;
	int heartbeats = 60;
    string mode = "http"; // http=HTTP 200 demo / echo=回显，用 -m 切换
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
        if ((strcmp(argv[i], "-m") == 0 || (strcmp(argv[i], "--mode") == 0)))
        {
            if (i + 1 >= argc)
            {
                LOG_DEBUG("no mode");
                break;
            }
            else
            {
                mode = argv[i + 1];
            }
        }
    }
    ThreadPool tp(threadnum);
    int sub_count = 5; // sub 个数,先写死跑通,以后加 -s 参数
    MainReactor main(port, tp, heartbeats, sub_count);
    if (mode == "echo")
    {
        main.setMessageHandler([](EpollServer &server, ConnectionId id, std::string_view msg)
                               {
                                   server.sendTo(id, msg); // 回显：原样发回
                               });
    }
    else
    {
        main.setMessageHandler([](EpollServer &server, ConnectionId id, std::string_view msg)
                               {
                                   std::string resp =
                                       "HTTP/1.1 200 OK\r\n"
                                       "Content-Length: 2\r\n"
                                       "Content-Type: text/plain\r\n"
                                       "\r\n"
                                       "ok";
                                   server.sendToRaw(id, resp); // HTTP 200
                               });
    }
    main.start();
    return 0;
}
