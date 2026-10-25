CC = gcc
CFLAGS = -Wall -Wextra -std=c11
PYTHON ?= python3

.PHONY: all clean test

all: ServerTrd trader analyst ticker

ServerTrd: ServerTrd.o
	$(CC) $(CFLAGS) -o $@ $^

trader: trader.o client_common.o
	$(CC) $(CFLAGS) -o $@ $^

analyst: analyst.o client_common.o
	$(CC) $(CFLAGS) -o $@ $^

ticker: ticker.o
	$(CC) $(CFLAGS) -o $@ $^

test: all
	$(PYTHON) -m unittest discover -s tests -v

ServerTrd.o: ServerTrd.c
	$(CC) $(CFLAGS) -c ServerTrd.c

trader.o: trader.c client_common.h
	$(CC) $(CFLAGS) -c trader.c

analyst.o: analyst.c client_common.h
	$(CC) $(CFLAGS) -c analyst.c

client_common.o: client_common.c client_common.h
	$(CC) $(CFLAGS) -c client_common.c

ticker.o: ticker.c
	$(CC) $(CFLAGS) -c ticker.c

clean:
	rm -f ServerTrd trader analyst ticker *.o
