/*
 * common.h - shared helpers for the CS4390 P2P tracker and peer programs.
 * Socket I/O helpers, config parsing, and an MD5 implementation (RFC 1321).
 */
#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdint.h>

#define MAXLINE      4096          /* longest protocol line we accept        */
#define CHUNK_SIZE   1024          /* max bytes per peer-to-peer GET (spec)   */
#define SEGMENT_SIZE (16 * 1024)   /* bytes per download segment (16 chunks)  */
#define MAX_PEERS    64            /* max peer entries per tracker file       */

/* Read one '\n'-terminated line (newline stripped). Returns length, 0 on EOF, -1 on error. */
int  read_line(int fd, char *buf, int max);
/* Write/read exactly n bytes. Return 0 on success, -1 on error/EOF. */
int  write_all(int fd, const void *buf, size_t n);
int  read_all(int fd, void *buf, size_t n);
/* Send a C string. */
int  send_str(int fd, const char *s);

/* Trim whitespace and an optional surrounding <...> from a protocol message, in place. */
void strip_message(char *s);

/* Open a TCP connection to ip:port. Returns socket fd or -1. */
int  connect_to(const char *ip, int port);
/* Create a listening TCP socket on the given port. Returns fd or -1. */
int  listen_on(int port);

/* Read line number `n` (1-based) of a config file into out. Returns 0 on success. */
int  config_line(const char *path, int n, char *out, int max);

/* MD5 helpers: hex digest written to out (33 bytes incl. NUL). */
void md5_buf(const unsigned char *data, size_t len, char out[33]);
int  md5_file(const char *path, char out[33]);

/* Current time as a "[HH:MM:SS]" prefix for log lines. */
const char *ts_now(void);

#endif
