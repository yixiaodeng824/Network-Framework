#include "epoll_server.h"
#include "Logger.h"
using namespace std;

int main() {
	logger::setLogLevel(logger::LOG_LEVEL_DEBUG);
	ThreadPool tp(4);
	EpollServer es(8888,tp);
	es.start();
	return 0;
}
