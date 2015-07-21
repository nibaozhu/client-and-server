/**********************************
 * author: nibz@qq.com
 * Mon May  4 21:00:16 CST 2015
 * $ ./receive 127.0.0.1 19999
 * ********************************/

#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>


#define MAX_EVENTS (0xff)
#define IP_LENGTH (0xf)
#define BUFFER_LENGTH (0xff)

int setnonblocking(int fd);
int reads(int fd);
int writes(int fd);
int do_use_fd();
int count = 0;
int quit = 0;
int fds[MAX_EVENTS] = {0};
int nconnect = 0;

int main(int argc, char **argv)
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		int fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		struct sockaddr_in addr;
		struct sockaddr_in local;
		memset(&addr, 0, sizeof (struct sockaddr_in));

		addr.sin_family = AF_INET;
		addr.sin_port = htons(atoi(argv[2]));
		retval = inet_pton(AF_INET, argv[1], (struct sockaddr *) &addr.sin_addr.s_addr);
		if (retval != 1)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		socklen_t addrlen = sizeof (struct sockaddr_in);
		retval = connect(fd, (struct sockaddr *) &addr, addrlen);
		if (retval == -1)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		fds[nconnect++] = fd;

		socklen_t locallen = sizeof (struct sockaddr_in);
		retval = getsockname(fd, (struct sockaddr *) &local, &locallen);
		if (retval == -1)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		char local_ip[IP_LENGTH];
		memset(local_ip, 0, sizeof local_ip);
		if (inet_ntop(AF_INET, (void*) &local.sin_addr.s_addr, local_ip, locallen) == NULL)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		//from ... to ...
		printf("local %s:%d, remote %s:%d\n", local_ip, ntohs(local.sin_port), argv[1], ntohs(addr.sin_port));

		//set non blocking
		retval = setnonblocking(fd);
		if (retval < 0)
		{
			break;
		}

		int efd = epoll_create(MAX_EVENTS);
		if (efd == -1)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		struct epoll_event ev, events[MAX_EVENTS];
		ev.events = EPOLLIN | EPOLLET; //epoll edge triggered
		//      ev.events = EPOLLIN | EPOLLOUT | EPOLLET; //epoll edge triggered
		//      ev.events = EPOLLIN | EPOLLOUT; //epoll level triggered (default)
		ev.data.fd = fd;
		retval = epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev);
		if (retval == -1)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		int n = 0;
		while (1)
		{
			if (quit == 1)
			{
				break;
			}

			int nfds = epoll_wait(efd, events, MAX_EVENTS, 1000);
			if (nfds == -1)
			{
				printf("%s\n", strerror(errno));
				break;
			}

			for (n = 0; n < nfds; n++)
			{
				if (events[n].events & EPOLLHUP)
				{
					printf("events[%d].events = 0x%03x\n", n, events[n].events);
					puts("Hang up happened on the associated file descriptor."); // my message
					retval = epoll_ctl(efd, EPOLL_CTL_DEL, events[n].data.fd, &events[n]);
					if (retval == -1)
					{
						printf("%s\n", strerror(errno));
						break;
					}
					close(events[n].data.fd);
					quit = 1;
					break;
				}

				if (events[n].events & EPOLLERR)
				{
					printf("0x%03x\n", events[n].events);
					puts("Error condition happened on the associated file descriptor.  epoll_wait(2) will always wait for this event; it is not necessary to set it in events."); // my message
					retval = epoll_ctl(efd, EPOLL_CTL_DEL, events[n].data.fd, &events[n]);
					if (retval == -1)
					{
						printf("%s\n", strerror(errno));
						break;
					}
					close(events[n].data.fd);
					quit = 1;
					break;
				}

				if (events[n].events & EPOLLIN)
				{
					printf("events[%d].events = 0x%03x\n", n, events[n].events);
					puts("The associated file is available for read(2) operations."); // my message
					retval = reads(events[n].data.fd);
					if (retval < 0)
					{
						break;
					}

					do_use_fd();
				}
			}
		}
	} while (0);
	return retval;
}

int reads(int fd)
{
	printf("%s:%u:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		char buffer[BUFFER_LENGTH];
		memset(buffer, 0, sizeof buffer);
		retval = read(fd, buffer, BUFFER_LENGTH);
		if (retval < 0)
		{
			printf("%s\n", strerror(errno));
			if (errno == EAGAIN)
			{
				printf("count = %d\n", count);
				quit = 1;
			}
			break;
		}
		printf("fd = %d, READ [%s]\n", fd, buffer);
	} while (0);
	count ++;
	return retval;
}

int writes(int fd)
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		char buffer[BUFFER_LENGTH];
		memset(buffer, 0, sizeof buffer);
		sprintf(buffer, "abcd");
		size_t length = strlen(buffer);
		retval = write(fd, buffer, length);
		if (retval < 0)
		{
			printf("%s\n", strerror(errno));
			break;
		}
		printf("fd = %d, write [%s]\n", fd, buffer);
	} while (0);
	return retval;
}

int setnonblocking(int fd)
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		int flags = fcntl(fd, F_GETFL);	
		if (flags == -1)
		{
			retval = flags;
			printf("%s\n", strerror(errno));
			break;
		}
		flags |= O_NONBLOCK; //non block
		retval = fcntl(fd, F_SETFL, flags);
		if (retval == -1)
		{
			printf("%s\n", strerror(errno));
			break;
		}
	} while (0);
	return retval;
}

int do_use_fd()
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		int n;
		for (n = 0; n < nconnect; n++)
		{
			retval = writes(fds[n]);
			if (retval < 0)
			{
				break;
			}
		}

	} while (0);
	return retval;
}

