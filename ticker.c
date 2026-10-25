#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static volatile sig_atomic_t g_stop = 0;

static void on_sigint(int signo)
{
    (void)signo;
    g_stop = 1;
}

static int parse_port(const char *text, int *port_out)
{
    char *end = NULL;
    long value;

    if (text == NULL || *text == '\0') {
        return -1;
    }

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || value < 1024 || value > 65535) {
        return -1;
    }

    *port_out = (int)value;
    return 0;
}

int main(int argc, char **argv)
{
    int port;
    int fd;
    int yes = 1;
    struct sockaddr_in addr;
    struct sigaction sa;

    if (argc != 2 || parse_port(argv[1], &port) != 0) {
        fprintf(stderr, "usage: ./ticker <udp_port>\n");
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction");
        return 1;
    }

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        close(fd);
        return 1;
    }
#ifdef SO_REUSEPORT
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
    if (setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes)) < 0) {
        perror("setsockopt SO_BROADCAST");
        close(fd);
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return 1;
    }

    printf("[TICKER] LISTENING udp_port=%d\n", port);
    fflush(stdout);

    while (!g_stop) {
        fd_set readfds;
        struct timeval tv;
        int ready;

        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        ready = select(fd + 1, &readfds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            close(fd);
            return 1;
        }

        if (ready > 0 && FD_ISSET(fd, &readfds)) {
            char buf[2048];
            ssize_t nread = recvfrom(fd, buf, sizeof(buf) - 1, 0, NULL, NULL);
            if (nread < 0) {
                if (errno == EINTR) {
                    continue;
                }
                perror("recvfrom");
                close(fd);
                return 1;
            }
            buf[nread] = '\0';
            while (nread > 0 && (buf[nread - 1] == '\n' || buf[nread - 1] == '\r')) {
                buf[nread - 1] = '\0';
                --nread;
            }
            printf("[TICKER] RECEIVED %s\n", buf);
            fflush(stdout);
        }
    }

    close(fd);
    return 0;
}
