/*
 * This is free software; see the source for copying conditions.  There is NO
 * warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * Written by Ni Baozhu: <nibz@qq.com>.
 */

#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>

#define MAX_EVENTS (0xff)
#define IP_LENGTH (0xf)
#define BUFFER_LENGTH (0xffff)

int setnonblocking(int fd);
int reads(int fd);
int writes(int fd, const char *uri, const char *host);
int do_use_fd();
int count = 0;
int quit = 0;
int fds[MAX_EVENTS] = {0};
int nconnect = 0;

/////////////////
char host[1024];
char uri[1024] = "/";

int usage(const char *argv0)
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	return printf("%s nibaozhu.cn 80 \"/\"\n", argv0);
}

int main(int argc, char **argv)
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		if (argc < 3)
		{
			usage(argv[0]);
			break;
		}

		int fd = socket(AF_INET, SOCK_STREAM, 0);
		if (fd < 0)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		///////////////
		memset(host, 0, sizeof host);
		memcpy(host, argv[1], sizeof host - 1);

		///////////////
		memset(uri, 0, sizeof uri);
		memcpy(uri, argv[3], sizeof uri - 1);

		struct sockaddr_in addr;
		struct sockaddr_in local;
		memset(&addr, 0, sizeof (struct sockaddr_in));

		addr.sin_family = AF_INET;
		addr.sin_port = htons(atoi(argv[2]));

		struct hostent *ht;
		ht = gethostbyname(argv[1]);
		if (ht == NULL)
		{
			herror("gethostbyname");
			break;
		}

		char temp_addr[1024] = {0};
		socklen_t slt = sizeof temp_addr;
		char *ri;

		// On success, inet_ntop() returns a non-null pointer to dst.  NULL is returned if there was an error, with errno set to indicate the error.
		inet_ntop(AF_INET, (void *)*(ht->h_addr_list), temp_addr, slt);
		if (temp_addr == NULL)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		retval = inet_pton(AF_INET, temp_addr, (struct sockaddr *) &addr.sin_addr.s_addr);
		if (retval == 0)
		{
			printf("0 is returned if src does not contain a character string representing a valid network address in the specified address family.\n");
			break;
		}
		else if (retval == -1)
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
		printf("local %s:%d, remote %s:%d\n", local_ip, ntohs(local.sin_port), temp_addr, ntohs(addr.sin_port));

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
		//      ev.events = EPOLLIN | EPOLLOUT | EPOLLET; // epoll edge triggered
		//      ev.events = EPOLLIN | EPOLLOUT          ; // epoll level triggered (default)
		ev.data.fd = fd;
		retval = epoll_ctl(efd, EPOLL_CTL_ADD, fd, &ev);
		if (retval == -1)
		{
			printf("%s\n", strerror(errno));
			break;
		}

		do_use_fd();

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

//					do_use_fd();
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
		printf("fd = %d\n---response begin---\n%s\n---response end---\n", fd, buffer);
	} while (0);
	count ++;
	return retval;
}

int writes(int fd, const char *uri, const char *host)
{
	printf("%s:%d:%s\n", __FILE__, __LINE__, __func__);
	int retval = 0;
	do {
		char buffer[BUFFER_LENGTH];
		memset(buffer, 0, sizeof buffer);

		sprintf(buffer,
/* -http protocol request message header START- */
"GET %s HTTP/1.1\r\n"
"User-Agent: Wget/1.12 (linux-gnu)\r\n"
"Accept: */*\r\n"
"Host: %s\r\n"
"Connection: Keep-Alive\r\n"
"\r\n"
/* -http protocol request message header ENDED- */
, uri, host);

		size_t length = strlen(buffer);
		retval = write(fd, buffer, length);
		if (retval < 0)
		{
			printf("%s\n", strerror(errno));
			break;
		}
		printf("fd = %d\n---request begin---\n%s---request end---\n", fd, buffer);
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
			retval = writes(fds[n], uri, host);
			if (retval < 0)
			{
				break;
			}
		}

	} while (0);
	return retval;
}

