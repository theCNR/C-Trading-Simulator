#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAX_CLIENTS 128
#define MAX_STOCKS 128
#define MAX_SYMBOL_LEN 8
#define MAX_USERNAME_LEN 31
#define MAX_LINE_LEN 512
#define OUTBUF_SIZE 16384
#define MESSAGE_SIZE 8192
#define PERIODIC_SECONDS 5

typedef struct {
    char symbol[MAX_SYMBOL_LEN + 1];
    long cents;
} stock_t;

typedef struct {
    int fd;
    char username[MAX_USERNAME_LEN + 1];
    char type[16];
    char line_buf[MAX_LINE_LEN + 1];
    size_t line_len;
    int too_long;
    long portfolio[MAX_STOCKS];
    char out_buf[OUTBUF_SIZE];
    size_t out_len;
    size_t out_sent;
    int closing_after_flush;
    char close_reason[16];
} client_t;

static stock_t g_stocks[MAX_STOCKS];
static size_t g_stock_count = 0;
static client_t g_clients[MAX_CLIENTS];
static int g_listen_fd = -1;
static int g_udp_fd = -1;
static int g_tcp_port = 0;
static int g_udp_port = 0;
static FILE *g_log_file = NULL;
static struct sockaddr_in g_broadcast_addr;
static time_t g_last_broadcast = 0;
static volatile sig_atomic_t g_shutdown_requested = 0;

static void on_sigint(int signo)
{
    (void)signo;
    g_shutdown_requested = 1;
}

static void usage(const char *prog)
{
    fprintf(stderr, "usage: %s -p <tcp_port> -u <udp_port> -s <stocks.txt> -l <logfile>\n", prog);
}

static void copy_string(char *dst, size_t dst_size, const char *src)
{
    size_t i = 0;

    if (dst_size == 0) {
        return;
    }

    while (i + 1 < dst_size && src[i] != '\0') {
        dst[i] = src[i];
        ++i;
    }
    dst[i] = '\0';
}

static void server_log(const char *fmt, ...)
{
    va_list ap;
    va_list ap_copy;

    va_start(ap, fmt);
    va_copy(ap_copy, ap);
    vfprintf(stdout, fmt, ap);
    fputc('\n', stdout);
    fflush(stdout);
    if (g_log_file != NULL) {
        vfprintf(g_log_file, fmt, ap_copy);
        fputc('\n', g_log_file);
        fflush(g_log_file);
    }
    va_end(ap_copy);
    va_end(ap);
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

static int valid_symbol(const char *symbol)
{
    size_t i;
    size_t len;

    if (symbol == NULL) {
        return 0;
    }

    len = strlen(symbol);
    if (len == 0 || len > MAX_SYMBOL_LEN) {
        return 0;
    }

    for (i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)symbol[i];
        if (!(isdigit(c) || isupper(c))) {
            return 0;
        }
    }

    return 1;
}

static int valid_username(const char *username)
{
    size_t i;
    size_t len;

    if (username == NULL) {
        return 0;
    }

    len = strlen(username);
    if (len == 0 || len > MAX_USERNAME_LEN) {
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

static int parse_price_cents(const char *text, long *cents_out)
{
    size_t i = 0;
    int dollars_digits = 0;
    long dollars = 0;
    long cents;

    if (text == NULL || *text == '\0') {
        return -1;
    }

    while (isdigit((unsigned char)text[i])) {
        long digit = text[i] - '0';
        if (dollars > (LONG_MAX - digit) / 10) {
            return -1;
        }
        dollars = dollars * 10 + digit;
        ++i;
        ++dollars_digits;
    }

    if (dollars_digits == 0 || text[i] != '.') {
        return -1;
    }
    ++i;

    if (!isdigit((unsigned char)text[i]) || !isdigit((unsigned char)text[i + 1])) {
        return -1;
    }
    cents = (long)(text[i] - '0') * 10 + (text[i + 1] - '0');
    i += 2;

    if (text[i] != '\0') {
        return -1;
    }

    if (dollars > (LONG_MAX - cents) / 100) {
        return -1;
    }
    *cents_out = dollars * 100 + cents;
    return *cents_out > 0 ? 0 : -1;
}

static void format_price(long cents, char *buf, size_t size)
{
    long whole = cents / 100;
    long frac = cents % 100;

    if (frac < 0) {
        frac = -frac;
    }
    (void)snprintf(buf, size, "%ld.%02ld", whole, frac);
}

static int find_stock(const char *symbol)
{
    size_t i;

    for (i = 0; i < g_stock_count; ++i) {
        if (strcmp(g_stocks[i].symbol, symbol) == 0) {
            return (int)i;
        }
    }

    return -1;
}

static int load_stocks(const char *path)
{
    FILE *fp;
    char line[256];

    fp = fopen(path, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char symbol[32];
        char price_text[64];
        char extra;
        long cents;

        if (sscanf(line, "%31s %63s %c", symbol, price_text, &extra) != 2) {
            continue;
        }
        if (!valid_symbol(symbol) || parse_price_cents(price_text, &cents) != 0) {
            continue;
        }
        if (find_stock(symbol) >= 0 || g_stock_count >= MAX_STOCKS) {
            continue;
        }

        copy_string(g_stocks[g_stock_count].symbol, sizeof(g_stocks[g_stock_count].symbol),
                    symbol);
        g_stocks[g_stock_count].cents = cents;
        ++g_stock_count;
    }

    if (fclose(fp) != 0) {
        return -1;
    }
    return g_stock_count > 0 ? 0 : -1;
}

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }

    return 0;
}

static void reset_client(client_t *client)
{
    memset(client, 0, sizeof(*client));
    client->fd = -1;
}

static void init_clients(void)
{
    size_t i;

    for (i = 0; i < MAX_CLIENTS; ++i) {
        reset_client(&g_clients[i]);
    }
}

static size_t active_client_count(void)
{
    size_t i;
    size_t count = 0;

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd >= 0) {
            ++count;
        }
    }

    return count;
}

static int find_free_client(void)
{
    size_t i;

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd < 0) {
            return (int)i;
        }
    }

    return -1;
}

static int username_taken(const char *username)
{
    size_t i;

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd >= 0 && g_clients[i].username[0] != '\0' &&
            strcmp(g_clients[i].username, username) == 0) {
            return 1;
        }
    }

    return 0;
}

static void close_client(size_t index, const char *reason)
{
    client_t *client = &g_clients[index];
    char username[MAX_USERNAME_LEN + 1];

    if (client->fd < 0) {
        return;
    }

    if (client->username[0] != '\0') {
        copy_string(username, sizeof(username), client->username);
    } else {
        copy_string(username, sizeof(username), "-");
    }

    close(client->fd);
    reset_client(client);

    if (reason != NULL) {
        server_log("[SERVER] CLIENT_DISCONNECTED username=%s reason=%s", username, reason);
    }
}

static int queue_response(client_t *client, const char *fmt, ...)
{
    char tmp[MESSAGE_SIZE];
    va_list ap;
    int n;
    size_t len;

    if (client->out_sent == client->out_len) {
        client->out_sent = 0;
        client->out_len = 0;
    }

    va_start(ap, fmt);
    n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);

    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        return -1;
    }

    len = (size_t)n;
    if (len + 1 > sizeof(tmp)) {
        return -1;
    }
    tmp[len] = '\n';
    ++len;

    if (len > OUTBUF_SIZE - client->out_len) {
        return -1;
    }

    memcpy(client->out_buf + client->out_len, tmp, len);
    client->out_len += len;
    return 0;
}

static int flush_client_output(size_t index, int log_error_disconnect)
{
    client_t *client = &g_clients[index];

    while (client->fd >= 0 && client->out_sent < client->out_len) {
        ssize_t n = send(client->fd, client->out_buf + client->out_sent,
                         client->out_len - client->out_sent, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            close_client(index, log_error_disconnect ? "hangup" : NULL);
            return -1;
        }
        if (n == 0) {
            return 0;
        }
        client->out_sent += (size_t)n;
    }

    if (client->fd >= 0 && client->out_sent == client->out_len) {
        client->out_sent = 0;
        client->out_len = 0;
        if (client->closing_after_flush) {
            char reason[sizeof(client->close_reason)];
            copy_string(reason, sizeof(reason), client->close_reason);
            close_client(index, reason);
        }
    }

    return 0;
}

static int appendf(char *buf, size_t size, size_t *len, const char *fmt, ...)
{
    va_list ap;
    int n;

    if (*len >= size) {
        return -1;
    }

    va_start(ap, fmt);
    n = vsnprintf(buf + *len, size - *len, fmt, ap);
    va_end(ap);

    if (n < 0 || (size_t)n >= size - *len) {
        return -1;
    }

    *len += (size_t)n;
    return 0;
}

static void broadcast_prices(const char *trigger)
{
    char message[MESSAGE_SIZE];
    size_t len = 0;
    size_t i;

    if (appendf(message, sizeof(message), &len, "PRICE_UPDATE") != 0) {
        return;
    }

    for (i = 0; i < g_stock_count; ++i) {
        char price[32];
        format_price(g_stocks[i].cents, price, sizeof(price));
        if (appendf(message, sizeof(message), &len, " %s:%s", g_stocks[i].symbol, price) != 0) {
            return;
        }
    }

    if (g_udp_fd >= 0) {
        (void)sendto(g_udp_fd, message, strlen(message), 0,
                     (struct sockaddr *)&g_broadcast_addr, sizeof(g_broadcast_addr));
    }
    g_last_broadcast = time(NULL);
    server_log("[SERVER] PRICE_BROADCAST trigger=%s", trigger);
}

static int create_tcp_listener(int port)
{
    int fd;
    int yes = 1;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        close(fd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 32) < 0) {
        close(fd);
        return -1;
    }
    if (set_nonblocking(fd) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static int create_udp_socket(int port)
{
    int fd;
    int yes = 1;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return -1;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes)) < 0) {
        close(fd);
        return -1;
    }
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if (set_nonblocking(fd) != 0) {
        close(fd);
        return -1;
    }

    memset(&g_broadcast_addr, 0, sizeof(g_broadcast_addr));
    g_broadcast_addr.sin_family = AF_INET;
    g_broadcast_addr.sin_port = htons((uint16_t)port);
    g_broadcast_addr.sin_addr.s_addr = inet_addr("255.255.255.255");

    return fd;
}

static void accept_ready_clients(void)
{
    for (;;) {
        struct sockaddr_in addr;
        socklen_t addr_len = sizeof(addr);
        int fd;
        int index;
        char ip[INET_ADDRSTRLEN];

        fd = accept(g_listen_fd, (struct sockaddr *)&addr, &addr_len);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            break;
        }

        if (fd >= FD_SETSIZE || set_nonblocking(fd) != 0) {
            close(fd);
            continue;
        }

        index = find_free_client();
        if (index < 0) {
            close(fd);
            continue;
        }

        reset_client(&g_clients[index]);
        g_clients[index].fd = fd;
        if (inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip)) == NULL) {
            copy_string(ip, sizeof(ip), "0.0.0.0");
        }
        server_log("[SERVER] CLIENT_CONNECTED fd=%d ip=%s", fd, ip);
    }
}

static int parse_qty(const char *text, long *qty_out)
{
    char *end = NULL;
    long value;

    if (text == NULL || *text == '\0') {
        return -1;
    }

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno == ERANGE || end == text || *end != '\0' || value <= 0) {
        return -1;
    }

    *qty_out = value;
    return 0;
}

static int tokenize(char *line, char **tokens, int max_tokens)
{
    int count = 0;
    char *save = NULL;
    char *tok = strtok_r(line, " \t", &save);

    while (tok != NULL && count < max_tokens) {
        tokens[count++] = tok;
        tok = strtok_r(NULL, " \t", &save);
    }

    if (tok != NULL) {
        return -1;
    }

    return count;
}

static int queue_report(client_t *client)
{
    char message[MESSAGE_SIZE];
    size_t len = 0;
    size_t i;

    if (appendf(message, sizeof(message), &len, "OK REPORT") != 0) {
        return -1;
    }

    for (i = 0; i < g_stock_count; ++i) {
        char price[32];
        const char *sep = (i == 0) ? " " : ",";
        format_price(g_stocks[i].cents, price, sizeof(price));
        if (appendf(message, sizeof(message), &len, "%s%s:%s", sep, g_stocks[i].symbol,
                    price) != 0) {
            return -1;
        }
    }

    return queue_response(client, "%s", message);
}

static int queue_list(client_t *client)
{
    char message[MESSAGE_SIZE];
    size_t len = 0;
    size_t i;
    int first = 1;

    if (appendf(message, sizeof(message), &len, "OK LIST") != 0) {
        return -1;
    }

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd >= 0 && g_clients[i].username[0] != '\0') {
            if (appendf(message, sizeof(message), &len, "%s%s", first ? " " : ",",
                        g_clients[i].username) != 0) {
                return -1;
            }
            first = 0;
        }
    }

    if (first) {
        if (appendf(message, sizeof(message), &len, " EMPTY") != 0) {
            return -1;
        }
    }

    return queue_response(client, "%s", message);
}

static int queue_portfolio(client_t *client)
{
    char message[MESSAGE_SIZE];
    size_t len = 0;
    size_t i;
    int first = 1;

    if (appendf(message, sizeof(message), &len, "OK PORTFOLIO") != 0) {
        return -1;
    }

    for (i = 0; i < g_stock_count; ++i) {
        if (client->portfolio[i] > 0) {
            if (appendf(message, sizeof(message), &len, "%s%s:%ld", first ? " " : ",",
                        g_stocks[i].symbol, client->portfolio[i]) != 0) {
                return -1;
            }
            first = 0;
        }
    }

    if (first) {
        if (appendf(message, sizeof(message), &len, " EMPTY") != 0) {
            return -1;
        }
    }

    return queue_response(client, "%s", message);
}

static void reject_or_close_on_queue_error(size_t index)
{
    if (g_clients[index].fd >= 0) {
        close_client(index, "hangup");
    }
}

static void handle_join(size_t index, char **tokens, int ntokens)
{
    client_t *client = &g_clients[index];

    if (ntokens != 3 ||
        (strcmp(tokens[1], "TRADER") != 0 && strcmp(tokens[1], "ANALYST") != 0) ||
        !valid_username(tokens[2])) {
        if (queue_response(client, "ERR UNKNOWN JOIN") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    if (username_taken(tokens[2])) {
        if (queue_response(client, "ERR JOIN name_taken") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    copy_string(client->type, sizeof(client->type), tokens[1]);
    copy_string(client->username, sizeof(client->username), tokens[2]);
    if (queue_response(client, "OK JOIN %s", client->username) != 0) {
        reject_or_close_on_queue_error(index);
        return;
    }
    server_log("[SERVER] JOIN username=%s type=%s fd=%d", client->username, client->type,
               client->fd);
}

static void handle_buy(size_t index, char **tokens, int ntokens)
{
    client_t *client = &g_clients[index];
    int stock_index;
    long qty;
    long old_price;
    long new_price;
    long new_shares;
    char old_text[32];
    char new_text[32];

    if (strcmp(client->type, "TRADER") != 0) {
        if (queue_response(client, "ERR UNAUTHORIZED") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    if (ntokens != 3 || parse_qty(tokens[2], &qty) != 0) {
        if (queue_response(client, "ERR UNKNOWN BUY") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    stock_index = find_stock(tokens[1]);
    if (stock_index < 0) {
        if (queue_response(client, "ERR UNKNOWN_SYMBOL") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    old_price = g_stocks[stock_index].cents;
    if (old_price <= 0 || client->portfolio[stock_index] < 0 ||
        qty > LONG_MAX - old_price || qty > LONG_MAX - client->portfolio[stock_index]) {
        if (queue_response(client, "ERR UNKNOWN BUY") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    new_price = old_price + qty;
    new_shares = client->portfolio[stock_index] + qty;

    format_price(old_price, old_text, sizeof(old_text));
    format_price(new_price, new_text, sizeof(new_text));
    if (queue_response(client, "OK BUY %s %ld %s", g_stocks[stock_index].symbol, qty,
                       new_text) != 0) {
        reject_or_close_on_queue_error(index);
        return;
    }

    g_stocks[stock_index].cents = new_price;
    client->portfolio[stock_index] = new_shares;
    server_log("[SERVER] BUY trader=%s symbol=%s qty=%ld old_price=%s new_price=%s",
               client->username, g_stocks[stock_index].symbol, qty, old_text, new_text);
    broadcast_prices("TRADE");
}

static void handle_sell(size_t index, char **tokens, int ntokens)
{
    client_t *client = &g_clients[index];
    int stock_index;
    long qty;
    long old_price;
    long new_price;
    long new_shares;
    char old_text[32];
    char new_text[32];

    if (strcmp(client->type, "TRADER") != 0) {
        if (queue_response(client, "ERR UNAUTHORIZED") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    if (ntokens != 3 || parse_qty(tokens[2], &qty) != 0) {
        if (queue_response(client, "ERR UNKNOWN SELL") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    stock_index = find_stock(tokens[1]);
    if (stock_index < 0) {
        if (queue_response(client, "ERR UNKNOWN_SYMBOL") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    if (client->portfolio[stock_index] < qty) {
        if (queue_response(client, "ERR INSUFFICIENT_SHARES") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    old_price = g_stocks[stock_index].cents;
    if (old_price < 1) {
        if (queue_response(client, "ERR UNKNOWN SELL") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    new_price = qty >= old_price ? 1 : old_price - qty;
    new_shares = client->portfolio[stock_index] - qty;

    format_price(old_price, old_text, sizeof(old_text));
    format_price(new_price, new_text, sizeof(new_text));
    if (queue_response(client, "OK SELL %s %ld %s", g_stocks[stock_index].symbol, qty,
                       new_text) != 0) {
        reject_or_close_on_queue_error(index);
        return;
    }

    g_stocks[stock_index].cents = new_price;
    client->portfolio[stock_index] = new_shares;
    server_log("[SERVER] SELL trader=%s symbol=%s qty=%ld old_price=%s new_price=%s",
               client->username, g_stocks[stock_index].symbol, qty, old_text, new_text);
    broadcast_prices("TRADE");
}

static void handle_price(size_t index, char **tokens, int ntokens)
{
    client_t *client = &g_clients[index];
    int stock_index;
    char price[32];

    if (strcmp(client->type, "ANALYST") != 0) {
        if (queue_response(client, "ERR UNAUTHORIZED") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    if (ntokens != 2) {
        if (queue_response(client, "ERR UNKNOWN PRICE") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    stock_index = find_stock(tokens[1]);
    if (stock_index < 0) {
        if (queue_response(client, "ERR UNKNOWN_SYMBOL") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    format_price(g_stocks[stock_index].cents, price, sizeof(price));
    if (queue_response(client, "OK PRICE %s %s", g_stocks[stock_index].symbol, price) != 0) {
        reject_or_close_on_queue_error(index);
    }
}

static void process_line(size_t index, const char *line)
{
    client_t *client = &g_clients[index];
    char copy[MAX_LINE_LEN + 1];
    char *tokens[8];
    int ntokens;

    copy_string(copy, sizeof(copy), line);
    ntokens = tokenize(copy, tokens, (int)(sizeof(tokens) / sizeof(tokens[0])));
    if (ntokens < 0) {
        if (queue_response(client, "ERR UNKNOWN %s", tokens[0]) != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }
    if (ntokens == 0) {
        if (queue_response(client, "ERR UNKNOWN") != 0) {
            reject_or_close_on_queue_error(index);
        }
        return;
    }

    if (client->type[0] == '\0') {
        if (strcmp(tokens[0], "JOIN") != 0) {
            if (queue_response(client, "ERR NOT_JOINED") != 0) {
                reject_or_close_on_queue_error(index);
            }
            return;
        }
        handle_join(index, tokens, ntokens);
        return;
    }

    if (strcmp(tokens[0], "QUIT") == 0) {
        if (ntokens != 1) {
            if (queue_response(client, "ERR UNKNOWN QUIT") != 0) {
                reject_or_close_on_queue_error(index);
            }
            return;
        }
        if (queue_response(client, "OK QUIT") != 0) {
            reject_or_close_on_queue_error(index);
            return;
        }
        client->closing_after_flush = 1;
        copy_string(client->close_reason, sizeof(client->close_reason), "QUIT");
    } else if (strcmp(tokens[0], "JOIN") == 0) {
        if (queue_response(client, "ERR UNKNOWN JOIN") != 0) {
            reject_or_close_on_queue_error(index);
        }
    } else if (strcmp(tokens[0], "BUY") == 0) {
        handle_buy(index, tokens, ntokens);
    } else if (strcmp(tokens[0], "SELL") == 0) {
        handle_sell(index, tokens, ntokens);
    } else if (strcmp(tokens[0], "PORTFOLIO") == 0) {
        if (strcmp(client->type, "TRADER") != 0) {
            if (queue_response(client, "ERR UNAUTHORIZED") != 0) {
                reject_or_close_on_queue_error(index);
            }
        } else if (ntokens != 1) {
            if (queue_response(client, "ERR UNKNOWN PORTFOLIO") != 0) {
                reject_or_close_on_queue_error(index);
            }
        } else if (queue_portfolio(client) != 0) {
            reject_or_close_on_queue_error(index);
        }
    } else if (strcmp(tokens[0], "PRICE") == 0) {
        handle_price(index, tokens, ntokens);
    } else if (strcmp(tokens[0], "REPORT") == 0) {
        if (strcmp(client->type, "ANALYST") != 0) {
            if (queue_response(client, "ERR UNAUTHORIZED") != 0) {
                reject_or_close_on_queue_error(index);
            }
        } else if (ntokens != 1) {
            if (queue_response(client, "ERR UNKNOWN REPORT") != 0) {
                reject_or_close_on_queue_error(index);
            }
        } else if (queue_report(client) != 0) {
            reject_or_close_on_queue_error(index);
        }
    } else if (strcmp(tokens[0], "LIST") == 0) {
        if (strcmp(client->type, "ANALYST") != 0) {
            if (queue_response(client, "ERR UNAUTHORIZED") != 0) {
                reject_or_close_on_queue_error(index);
            }
        } else if (ntokens != 1) {
            if (queue_response(client, "ERR UNKNOWN LIST") != 0) {
                reject_or_close_on_queue_error(index);
            }
        } else if (queue_list(client) != 0) {
            reject_or_close_on_queue_error(index);
        }
    } else {
        if (queue_response(client, "ERR UNKNOWN %s", tokens[0]) != 0) {
            reject_or_close_on_queue_error(index);
        }
    }
}

static void handle_client_bytes(size_t index, const char *buf, ssize_t nread)
{
    client_t *client = &g_clients[index];
    ssize_t i;

    for (i = 0; client->fd >= 0 && i < nread; ++i) {
        unsigned char c = (unsigned char)buf[i];

        if (client->closing_after_flush) {
            return;
        }
        if (c == '\r') {
            continue;
        }
        if (c == '\n') {
            if (client->too_long) {
                client->too_long = 0;
                client->line_len = 0;
                if (queue_response(client, "ERR TOOLONG") != 0) {
                    reject_or_close_on_queue_error(index);
                }
            } else {
                client->line_buf[client->line_len] = '\0';
                process_line(index, client->line_buf);
                if (client->fd >= 0) {
                    client->line_len = 0;
                }
            }
            continue;
        }
        if (client->too_long) {
            continue;
        }
        if (client->line_len >= MAX_LINE_LEN) {
            client->too_long = 1;
            client->line_len = 0;
            continue;
        }
        client->line_buf[client->line_len++] = (char)c;
    }
}

static void read_client(size_t index)
{
    char buf[1024];
    client_t *client = &g_clients[index];

    for (;;) {
        ssize_t nread = recv(client->fd, buf, sizeof(buf), 0);
        if (nread < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            close_client(index, "hangup");
            break;
        }
        if (nread == 0) {
            close_client(index, "hangup");
            break;
        }

        handle_client_bytes(index, buf, nread);
        if (g_clients[index].fd < 0 || g_clients[index].closing_after_flush) {
            break;
        }
    }
}

static int parse_args(int argc, char **argv, const char **stocks_path, const char **log_path)
{
    int opt;
    int have_p = 0;
    int have_u = 0;
    int have_s = 0;
    int have_l = 0;

    while ((opt = getopt(argc, argv, "p:u:s:l:")) != -1) {
        switch (opt) {
        case 'p':
            if (parse_port(optarg, &g_tcp_port) != 0) {
                return -1;
            }
            have_p = 1;
            break;
        case 'u':
            if (parse_port(optarg, &g_udp_port) != 0) {
                return -1;
            }
            have_u = 1;
            break;
        case 's':
            *stocks_path = optarg;
            have_s = 1;
            break;
        case 'l':
            *log_path = optarg;
            have_l = 1;
            break;
        default:
            return -1;
        }
    }

    if (optind != argc || !have_p || !have_u || !have_s || !have_l) {
        return -1;
    }

    return 0;
}

static void prepare_fd_sets(fd_set *readfds, fd_set *writefds, int *maxfd)
{
    size_t i;

    FD_ZERO(readfds);
    FD_ZERO(writefds);
    *maxfd = g_listen_fd;
    FD_SET(g_listen_fd, readfds);

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd >= 0) {
            if (!g_clients[i].closing_after_flush) {
                FD_SET(g_clients[i].fd, readfds);
            }
            if (g_clients[i].out_sent < g_clients[i].out_len) {
                FD_SET(g_clients[i].fd, writefds);
            }
            if (g_clients[i].fd > *maxfd) {
                *maxfd = g_clients[i].fd;
            }
        }
    }
}

static void close_all_clients_without_log(void)
{
    size_t i;

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd >= 0) {
            close_client(i, NULL);
        }
    }
}

static void notify_clients_shutdown(void)
{
    size_t i;
    time_t deadline;

    for (i = 0; i < MAX_CLIENTS; ++i) {
        if (g_clients[i].fd >= 0) {
            (void)queue_response(&g_clients[i], "SERVER SHUTDOWN");
        }
    }

    deadline = time(NULL) + 2;
    while (active_client_count() > 0 && time(NULL) <= deadline) {
        fd_set writefds;
        int maxfd = -1;
        struct timeval tv;
        int ready;
        int any_pending = 0;

        FD_ZERO(&writefds);
        for (i = 0; i < MAX_CLIENTS; ++i) {
            if (g_clients[i].fd >= 0 && g_clients[i].out_sent < g_clients[i].out_len) {
                FD_SET(g_clients[i].fd, &writefds);
                if (g_clients[i].fd > maxfd) {
                    maxfd = g_clients[i].fd;
                }
                any_pending = 1;
            }
        }

        if (!any_pending) {
            break;
        }

        tv.tv_sec = 0;
        tv.tv_usec = 200000;
        ready = select(maxfd + 1, NULL, &writefds, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0) {
            continue;
        }
        for (i = 0; i < MAX_CLIENTS; ++i) {
            if (g_clients[i].fd >= 0 && FD_ISSET(g_clients[i].fd, &writefds)) {
                (void)flush_client_output(i, 0);
            }
        }
    }
}

static void cleanup_server(void)
{
    notify_clients_shutdown();
    close_all_clients_without_log();

    if (g_listen_fd >= 0) {
        close(g_listen_fd);
        g_listen_fd = -1;
    }
    if (g_udp_fd >= 0) {
        close(g_udp_fd);
        g_udp_fd = -1;
    }

    server_log("[SERVER] CLEANUP_DONE clients=0");
    if (g_log_file != NULL) {
        fclose(g_log_file);
        g_log_file = NULL;
    }
}

int main(int argc, char **argv)
{
    const char *stocks_path = NULL;
    const char *log_path = NULL;
    struct sigaction sa;

    init_clients();

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        perror("sigaction SIGPIPE");
        return 1;
    }

    if (parse_args(argc, argv, &stocks_path, &log_path) != 0) {
        usage(argv[0]);
        return 1;
    }
    if (load_stocks(stocks_path) != 0) {
        usage(argv[0]);
        return 1;
    }

    g_log_file = fopen(log_path, "w");
    if (g_log_file == NULL) {
        usage(argv[0]);
        return 1;
    }

    g_listen_fd = create_tcp_listener(g_tcp_port);
    g_udp_fd = create_udp_socket(g_udp_port);
    if (g_listen_fd < 0 || g_udp_fd < 0) {
        perror("socket setup");
        cleanup_server();
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction");
        cleanup_server();
        return 1;
    }

    g_last_broadcast = time(NULL);
    server_log("[SERVER] SERVER_START tcp_port=%d udp_port=%d stocks=%zu", g_tcp_port,
               g_udp_port, g_stock_count);

    while (!g_shutdown_requested) {
        fd_set readfds;
        fd_set writefds;
        int maxfd;
        int ready;
        time_t now = time(NULL);
        long elapsed = (long)(now - g_last_broadcast);
        long remaining = PERIODIC_SECONDS - elapsed;
        struct timeval tv;
        size_t i;

        if (remaining <= 0) {
            broadcast_prices("PERIODIC");
            continue;
        }

        tv.tv_sec = remaining;
        tv.tv_usec = 0;
        prepare_fd_sets(&readfds, &writefds, &maxfd);

        ready = select(maxfd + 1, &readfds, &writefds, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            break;
        }

        if (ready == 0) {
            broadcast_prices("PERIODIC");
            continue;
        }

        if (FD_ISSET(g_listen_fd, &readfds)) {
            accept_ready_clients();
        }

        for (i = 0; i < MAX_CLIENTS; ++i) {
            if (g_clients[i].fd >= 0 && FD_ISSET(g_clients[i].fd, &writefds)) {
                (void)flush_client_output(i, 1);
            }
        }

        for (i = 0; i < MAX_CLIENTS; ++i) {
            if (g_clients[i].fd >= 0 && FD_ISSET(g_clients[i].fd, &readfds)) {
                read_client(i);
            }
        }
    }

    server_log("[SERVER] SHUTDOWN signal=SIGINT");
    cleanup_server();
    return 0;
}
