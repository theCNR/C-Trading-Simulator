#ifndef CLIENT_COMMON_H
#define CLIENT_COMMON_H

#include <stddef.h>

#define CLIENT_LINE_MAX 512

int parse_port(const char *text, int *port_out);
int valid_username_arg(const char *username);
int connect_tcp(const char *server_ip, int port);
int send_all(int fd, const char *buf, size_t len);
int send_line(int fd, const char *line);
void strip_newline(char *s);
int run_tcp_client(const char *role, const char *server_ip, int port, const char *username);

#endif
