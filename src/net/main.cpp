#include "epoll_server.h"
#include "main_reactor.h"
#include "Logger.h"
#include <cstdlib>
#include <cstring>
using namespace std;

int main(int argc,char* argv[]) {
	int port = 8888;
	int threadnum = 0;
	int heartbeats = 60;
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
	}
    ThreadPool tp(threadnum);
    int sub_count = 2; // sub 个数,先写死跑通,以后加 -s 参数
    MainReactor main(port, tp, heartbeats, sub_count);
    main.setMessageHandler([](EpollServer &server, ConnectionId id, const std::string &msg)
                           { server.sendTo(id, msg); });
    main.start();
    return 0;
}
