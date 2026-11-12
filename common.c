/*
 * common.c - shared helpers for the CS4390 P2P tracker and peer programs.
 * The MD5 code below is our own implementation of RFC 1321
 * (https://www.ietf.org/rfc/rfc1321.txt); no external libraries are used.
 */
#include "common.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ---------------------------------------------------------------- socket I/O */

int read_line(int fd, char *buf, int max)
{
    int n = 0;
    char c;
    while (n < max - 1) {
        ssize_t r = read(fd, &c, 1);
        if (r == 0) break;                       /* peer closed connection */
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (c == '\n') { buf[n] = '\0'; return n + 1; }
        buf[n++] = c;
    }
    buf[n] = '\0';
    return n;
}

int write_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        p += w; n -= (size_t)w;
    }
    return 0;
}

int read_all(int fd, void *buf, size_t n)
{
    char *p = buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r == 0) return -1;                   /* unexpected EOF */
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        p += r; n -= (size_t)r;
    }
    return 0;
}

int send_str(int fd, const char *s) { return write_all(fd, s, strlen(s)); }

void strip_message(char *s)
{
    char *start = s;
    while (isspace((unsigned char)*start)) start++;
    if (*start == '<') start++;
    size_t len = strlen(start);
    while (len > 0 && isspace((unsigned char)start[len - 1])) len--;
    if (len > 0 && start[len - 1] == '>') len--;
    while (len > 0 && isspace((unsigned char)start[len - 1])) len--;
    memmove(s, start, len);
    s[len] = '\0';
}

int connect_to(const char *ip, int port)
{
    struct sockaddr_in addr;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    int one = 1;                                 /* send small requests immediately */
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int listen_on(int port)
{
    struct sockaddr_in addr;
    int fd = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
    if (fd < 0) return -1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0 || listen(fd, 32) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int config_line(const char *path, int n, char *out, int max)
{
    FILE *f = fopen(path, "r");
    char line[MAXLINE];
    int i = 0;
    if (!f) return -1;
    while (fgets(line, sizeof line, f)) {
        if (++i == n) {
            line[strcspn(line, "\r\n")] = '\0';
            snprintf(out, (size_t)max, "%s", line);
            fclose(f);
            return 0;
        }
    }
    fclose(f);
    return -1;
}

const char *ts_now(void)
{
    static __thread char buf[16];
    time_t t = time(NULL);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(buf, sizeof buf, "[%H:%M:%S]", &tmv);
    return buf;
}

/* ---------------------------------------------------------------- MD5 (RFC 1321) */

typedef struct {
    uint32_t h[4];
    uint64_t total;
    unsigned char buf[64];
    size_t used;
} md5_ctx;

static uint32_t K[64];
static const int S[64] = {
    7,12,17,22, 7,12,17,22, 7,12,17,22, 7,12,17,22,
    5, 9,14,20, 5, 9,14,20, 5, 9,14,20, 5, 9,14,20,
    4,11,16,23, 4,11,16,23, 4,11,16,23, 4,11,16,23,
    6,10,15,21, 6,10,15,21, 6,10,15,21, 6,10,15,21 };

#define ROTL(x, c) (((x) << (c)) | ((x) >> (32 - (c))))

/* Build the K table once at program start, before any threads exist. */
__attribute__((constructor)) static void md5_table(void)
{
    for (int i = 0; i < 64; i++)                 /* K[i] = floor(|sin(i+1)| * 2^32) */
        K[i] = (uint32_t)(fabs(sin(i + 1.0)) * 4294967296.0);
}

static void md5_init(md5_ctx *c)
{
    c->h[0] = 0x67452301; c->h[1] = 0xefcdab89; c->h[2] = 0x98badcfe; c->h[3] = 0x10325476;
    c->total = 0; c->used = 0;
}

static void md5_block(md5_ctx *c, const unsigned char *p)
{
    uint32_t w[16], a = c->h[0], b = c->h[1], cc = c->h[2], d = c->h[3];
    for (int i = 0; i < 16; i++)
        w[i] = (uint32_t)p[4*i] | (uint32_t)p[4*i+1] << 8 | (uint32_t)p[4*i+2] << 16 | (uint32_t)p[4*i+3] << 24;
    for (int i = 0; i < 64; i++) {
        uint32_t f; int g;
        if (i < 16)      { f = (b & cc) | (~b & d);  g = i; }
        else if (i < 32) { f = (d & b) | (~d & cc);  g = (5*i + 1) % 16; }
        else if (i < 48) { f = b ^ cc ^ d;           g = (3*i + 5) % 16; }
        else             { f = cc ^ (b | ~d);        g = (7*i) % 16; }
        uint32_t t = d; d = cc; cc = b;
        b = b + ROTL(a + f + K[i] + w[g], S[i]);
        a = t;
    }
    c->h[0] += a; c->h[1] += b; c->h[2] += cc; c->h[3] += d;
}

static void md5_update(md5_ctx *c, const unsigned char *data, size_t len)
{
    c->total += len;
    while (len > 0) {
        size_t take = 64 - c->used;
        if (take > len) take = len;
        memcpy(c->buf + c->used, data, take);
        c->used += take; data += take; len -= take;
        if (c->used == 64) { md5_block(c, c->buf); c->used = 0; }
    }
}

static void md5_final(md5_ctx *c, char out[33])
{
    uint64_t bits = c->total * 8;
    unsigned char pad = 0x80, zero = 0, lenb[8];
    md5_update(c, &pad, 1);
    while (c->used != 56) md5_update(c, &zero, 1);
    for (int i = 0; i < 8; i++) lenb[i] = (unsigned char)(bits >> (8 * i));
    md5_update(c, lenb, 8);
    for (int i = 0; i < 16; i++)
        sprintf(out + 2*i, "%02x", (c->h[i / 4] >> (8 * (i % 4))) & 0xff);
    out[32] = '\0';
}

void md5_buf(const unsigned char *data, size_t len, char out[33])
{
    md5_ctx c;
    md5_init(&c);
    md5_update(&c, data, len);
    md5_final(&c, out);
}

int md5_file(const char *path, char out[33])
{
    unsigned char buf[8192];
    size_t n;
    md5_ctx c;
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    md5_init(&c);
    while ((n = fread(buf, 1, sizeof buf, f)) > 0) md5_update(&c, buf, n);
    fclose(f);
    md5_final(&c, out);
    return 0;
}
