# CS4390 P2P project makefile
#   make -f makefile        builds ./tracker and peer1/peer, peer2/peer, peer3/peer
#   make clean              removes executables
CC      = gcc
CFLAGS  = -Wall -Wextra -Wno-format-truncation -O2 -pthread
LDLIBS  = -lm
PEERS   = peer1 peer2 peer3

all: tracker peers

tracker: tracker.c common.c common.h
	$(CC) $(CFLAGS) -o tracker tracker.c common.c $(LDLIBS)

peers: peer.c common.c common.h
	for p in $(PEERS); do mkdir -p $$p && $(CC) $(CFLAGS) -o $$p/peer peer.c common.c $(LDLIBS); done

clean:
	rm -f tracker $(addsuffix /peer,$(PEERS))

.PHONY: all peers clean
