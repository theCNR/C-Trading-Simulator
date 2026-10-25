#define _POSIX_C_SOURCE 200809L

#include "client_common.h"

#include <arpa/inet.h>
#include <ctype.h>
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

int parse_port(const char *text, int *port_out)
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

int valid_username_arg(const char *username)
{
    size_t i;
    size_t len;

    if (username == NULL) {
        return 0;
    }

    len = strlen(username);
    if (len == 0 || len > 31) {
        return 0;
    }

    for (i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)username[i];
        if (!(isalnum(c) || c == '_' || c == '-')) {
            return 0;
        }
    }

    return 1;
}

int connect_tcp(const char *server_ip, int port)
{
    int fd;
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, server_ip, &addr.sin_addr) != 1) {
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

int send_all(int fd, const char *buf, size_t len)
{
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        sent += (size_t)n;
    }

    return 0;
}

int send_line(int fd, const char *line)
{
    char buf[CLIENT_LINE_MAX + 2];
    size_t len = strlen(line);

    if (len > CLIENT_LINE_MAX) {
        return -1;
    }

    memcpy(buf, line, len);
    buf[len] = '\n';
    return send_all(fd, buf, len + 1);
}

void strip_newline(char *s)
{
    size_t len;

    if (s == NULL) {
        return;
    }

    len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r')) {
        s[len - 1] = '\0';
        --len;
    }
}

static void print_prefix(const char *role, const char *username, const char *keyword)
{
    printf("[%s %s] %s ", role, username, keyword);
}

static void print_disconnected_once(const char *role, const char *username, const char *reason,
                                    int *printed)
{
    if (*printed) {
        return;
    }
    printf("[%s %s] DISCONNECTED reason=%s\n", role, username, reason);
    fflush(stdout);
    *printed = 1;
}

static void process_server_bytes(const char *role, const char *username, const char *buf,
                                 ssize_t nread, char *line_buf, size_t *line_len,
                                 int *shutdown_seen)
{
    ssize_t i;

    for (i = 0; i < nread; ++i) {
        unsigned char c = (unsigned char)buf[i];
        if (c == '\r') {
            continue;
        }
        if (c == '\n') {
            line_buf[*line_len] = '\0';
            printf("[%s %s] RECEIVED %s\n", role, username, line_buf);
            fflush(stdout);
            if (strcmp(line_buf, "SERVER SHUTDOWN") == 0) {
                *shutdown_seen = 1;
            }
            *line_len = 0;
            continue;
        }
        if (*line_len + 1 < CLIENT_LINE_MAX * 4) {
            line_buf[*line_len] = (char)c;
            ++(*line_len);
        }
    }
}

int run_tcp_client(const char *role, const char *server_ip, int port, const char *username)
{
    int fd;
    char join_line[CLIENT_LINE_MAX + 1];
    char stdin_line[CLIENT_LINE_MAX + 1];
    char recv_buf[1024];
    char server_line[CLIENT_LINE_MAX * 4];
    size_t server_line_len = 0;
    int quit_sent = 0;
    int shutdown_seen = 0;
    int disconnected_printed = 0;
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        perror("sigaction SIGPIPE");
        return 1;
    }

    fd = connect_tcp(server_ip, port);
    if (fd < 0) {
        fprintf(stderr, "connect failed\n");
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction");
        close(fd);
        return 1;
    }

    printf("[%s %s] CONNECTED server=%s:%d\n", role, username, server_ip, port);
    fflush(stdout);

    if (strlen(role) + strlen(username) + 6 >= sizeof(join_line)) {
        close(fd);
        return 1;
    }
    strcpy(join_line, "JOIN ");
    strcat(join_line, role);
    strcat(join_line, " ");
    strcat(join_line, username);
    if (send_line(fd, join_line) < 0) {
        close(fd);
        return 1;
    }
    printf("[%s %s] SENT JOIN\n", role, username);
    fflush(stdout);

    while (!disconnected_printed) {
        fd_set readfds;
        int maxfd = fd;
        int ready;

        if (g_stop && !quit_sent) {
            if (send_line(fd, "QUIT") == 0) {
                printf("[%s %s] SENT QUIT\n", role, username);
                fflush(stdout);
            }
            quit_sent = 1;
        }

        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);
        if (!quit_sent) {
            FD_SET(STDIN_FILENO, &readfds);
            if (STDIN_FILENO > maxfd) {
                maxfd = STDIN_FILENO;
            }
        }

        ready = select(maxfd + 1, &readfds, NULL, NULL, NULL);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            close(fd);
            return 1;
        }

        if (!quit_sent && FD_ISSET(STDIN_FILENO, &readfds)) {
            if (fgets(stdin_line, sizeof(stdin_line), stdin) == NULL) {
                if (send_line(fd, "QUIT") == 0) {
                    printf("[%s %s] SENT QUIT\n", role, username);
                    fflush(stdout);
                }
                quit_sent = 1;
            } else {
                strip_newline(stdin_line);
                if (stdin_line[0] != '\0') {
                    if (send_line(fd, stdin_line) < 0) {
                        print_disconnected_once(role, username, "shutdown", &disconnected_printed);
                        break;
                    }
                    print_prefix(role, username, "SENT");
                    printf("%s\n", stdin_line);
                    fflush(stdout);
                    if (strcmp(stdin_line, "QUIT") == 0) {
                        quit_sent = 1;
                    }
                }
            }
        }

        if (FD_ISSET(fd, &readfds)) {
            ssize_t nread = recv(fd, recv_buf, sizeof(recv_buf), 0);
            if (nread < 0) {
                if (errno == EINTR) {
                    continue;
                }
                print_disconnected_once(role, username, "shutdown", &disconnected_printed);
                break;
            }
            if (nread == 0) {
                print_disconnected_once(role, username, quit_sent ? "QUIT" : "shutdown",
                                        &disconnected_printed);
                break;
            }

            process_server_bytes(role, username, recv_buf, nread, server_line, &server_line_len,
                                 &shutdown_seen);
            if (shutdown_seen) {
                print_disconnected_once(role, username, "shutdown", &disconnected_printed);
                break;
            }
            if (quit_sent) {
                print_disconnected_once(role, username, "QUIT", &disconnected_printed);
                break;
            }
        }
    }

    close(fd);
    return 0;
}
