#include "epoll_server.h"
#include "Logger.h"
#include <cstdlib>
#include <cstring>
using namespace std;

int main(int argc,char* argv[]) {
	logger::setLogLevel(logger::LOG_LEVEL_DEBUG);
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
	EpollServer es(port, tp,heartbeats);
	es.setMessageHandler([](EpollServer& server, int fd, const std::string& msg) {
		server.broadcast(fd,msg);
		});
	es.start();
	return 0;
}
