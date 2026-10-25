# Concurrent Stock Exchange Simulator

A small C demonstration of a single-threaded stock exchange server, TCP trader and analyst clients, and a UDP price ticker. It is intended for learning and local demonstrations; it is not a real exchange or a production trading service.

## Components

- `ServerTrd`: accepts IPv4 TCP clients, processes commands with `select()`, and broadcasts price snapshots over UDP.
- `trader`: sends trader commands over TCP.
- `analyst`: sends analyst queries over TCP.
- `ticker`: receives UDP price snapshots and prints them.

The server accepts up to 128 TCP clients and up to 128 stock symbols. TCP commands are newline-delimited and limited to 512 bytes. Usernames are 1-31 letters, digits, underscores, or hyphens. Stock symbols are 1-8 uppercase letters or digits.

## Build

Build on a POSIX environment with GCC, GNU Make, and Python 3:

```sh
make
make test
```

`make test` runs the local TCP/UDP integration tests. UDP broadcast behavior depends on the host network configuration.

## Run locally

Copy the sample data to a local file, then start each program in a separate terminal:

```sh
cp stocks.example.txt stocks.txt
./ServerTrd -p 8080 -u 8081 -s stocks.txt -l server.log
```

```sh
./trader 127.0.0.1 8080 demo-trader
./analyst 127.0.0.1 8080 demo-analyst
./ticker 8081
```

The server's `-p` port is its TCP port. Its `-u` port is the destination port for UDP broadcasts. The ticker listens on that UDP port. Both port arguments must be in the range 1024-65535.

The path supplied with `-l` is opened in write mode, so an existing log at that path is truncated when the server starts.

## Stock data format

Each nonblank record contains one symbol and one positive price with exactly two decimal places, separated by whitespace:

```text
ALPHA 100.00
BETA 50.00
```

Invalid and duplicate records are skipped. At least one valid record is required. Prices are stored as integer cents. These sample values are illustrative and are not current market data.

## TCP protocol

Every command and response is one newline-terminated line.

| Role | Commands |
| --- | --- |
| Either | `JOIN TRADER <username>` or `JOIN ANALYST <username>`, then `QUIT` |
| Trader | `BUY <symbol> <qty>`, `SELL <symbol> <qty>`, `PORTFOLIO` |
| Analyst | `PRICE <symbol>`, `REPORT`, `LIST` |

`qty` must be a positive integer representable by the platform's `long` type and must fit the current price and portfolio limits. A successful BUY increases the symbol's price by `qty` cents. A successful SELL decreases it by `qty` cents, with a floor of 1 cent. An individual trader's portfolio is held only for that connection and is cleared when the client disconnects.

The server returns `ERR TOOLONG` for an overlong command. Role violations return `ERR UNAUTHORIZED`; an attempted sale above the connected trader's holdings returns `ERR INSUFFICIENT_SHARES`.

## UDP ticker

The server broadcasts a datagram in this form:

```text
PRICE_UPDATE AAPL:150.00 MSFT:380.00
```

It sends a snapshot after a successful trade and every five seconds. The destination is the IPv4 limited broadcast address `255.255.255.255`; receivers generally need to be on the same broadcast domain.

## Security and privacy

This example has no account authentication or transport encryption. The role in `JOIN` is self-selected, and the TCP listener binds to all IPv4 interfaces. Run it only on a trusted, isolated network; do not expose it to the public internet or use it for real transactions. The UDP ticker is also unauthenticated.

The assignment report containing personal information is intentionally not part of the project distribution. The local report file is excluded by `.gitignore`.

## License

No license has been selected or included. The copyright holder should choose the reuse terms before publishing the source.
