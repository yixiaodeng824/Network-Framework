#include "epoll_server.h"
using namespace std;

int main() {
	ThreadPool tp(4);
	EpollServer es(8888,tp);
	es.start();
	return 0;
}
