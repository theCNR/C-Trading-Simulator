#include "client_common.h"

#include <stdio.h>

int main(int argc, char **argv)
{
    int port;

    if (argc != 4 || parse_port(argv[2], &port) != 0 || !valid_username_arg(argv[3])) {
        fprintf(stderr, "usage: ./trader <server_ip> <tcp_port> <username>\n");
        return 1;
    }

    return run_tcp_client("TRADER", argv[1], port, argv[3]);
}
