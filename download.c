#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "download.h"

#define MAX_PEERS      64
#define IO_TIMEOUT_SEC 5
#define MAX_THREADS    32

/* ---------- data ---------- */

typedef struct {
    char ip[64];
    int  port;
    long start, end;   /* bytes the peer holds: [start, end)  (end == filesize for a full copy) */
    long ts;           /* timestamp from the tracker file */
    int  dead;         /* set when we can't connect at all */
} peer_t;

typedef struct {
    char   filename[256];
    long   filesize;
    char   md5[64];
    peer_t peers[MAX_PEERS];
    int    npeers;
} track_t;

typedef struct {
    track_t track;
    char    outpath[600];
    int     fd;
    long    nseg;
    char   *seg_done;
    long    next_seg;
    int     failed;
    pthread_mutex_t mu;      /* protects next_seg, seg_done, failed, peer.dead */
    pthread_mutex_t upd_mu;  /* serialises updatetracker messages */
    long    last_reported;
    const char *my_ip;
    int     my_port;
    const char *tracker_ip;
    int     tracker_port;
} dl_t;

typedef struct { dl_t *d; int id; } worker_arg_t;

/* ---------- small helpers ---------- */

static void dl_rstrip(char *s)
{
    size_t n = strlen(s);
    while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r' || s[n-1] == ' '))
        s[--n] = '\0';
}

static char *dl_after_colon(char *line)
{
    char *p = strchr(line, ':');
    if (!p) return NULL;
    p++;
    while (*p == ' ') p++;
    return p;
}

static int dl_pwrite_all(int fd, const char *buf, size_t n, off_t off)
{
    while (n > 0) {
        ssize_t w = pwrite(fd, buf, n, off);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        buf += w; n -= (size_t)w; off += w;
    }
    return 0;
}

static int dl_send_all(int fd, const char *buf, size_t n)
{
    while (n > 0) {
        ssize_t w = send(fd, buf, n, MSG_NOSIGNAL);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        buf += w; n -= (size_t)w;
    }
    return 0;
}

/* TCP connect with send/receive timeouts so a dead peer can't hang a thread. */
static int dl_connect(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct timeval tv = { IO_TIMEOUT_SEC, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);  /* also bounds connect() on Linux */

    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &a.sin_addr) != 1 ||
        connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* ---------- tracker file parsing ---------- */

static int dl_parse_track(const char *path, track_t *t)
{
    FILE *f = fopen(path, "r");
    if (!f) { perror("open tracker file"); return -1; }
    memset(t, 0, sizeof *t);

    char line[1024];
    int got_name = 0, got_size = 0, got_md5 = 0;
    while (fgets(line, sizeof line, f)) {
        dl_rstrip(line);
        if (line[0] == '\0' || line[0] == '#' || line[0] == '<') continue;

        char *v;
        if (strncmp(line, "Filename:", 9) == 0 && (v = dl_after_colon(line))) {
            snprintf(t->filename, sizeof t->filename, "%s", v); got_name = 1;
        } else if (strncmp(line, "Filesize:", 9) == 0 && (v = dl_after_colon(line))) {
            t->filesize = atol(v); got_size = 1;
        } else if (strncmp(line, "MD5:", 4) == 0 && (v = dl_after_colon(line))) {
            snprintf(t->md5, sizeof t->md5, "%s", v); got_md5 = 1;
        } else if (strncmp(line, "Description:", 12) == 0) {
            /* not needed for downloading */
        } else if (t->npeers < MAX_PEERS) {
            peer_t *p = &t->peers[t->npeers];
            if (sscanf(line, "%63[^:]:%d:%ld:%ld:%ld",
                       p->ip, &p->port, &p->start, &p->end, &p->ts) == 5)
                t->npeers++;
        }
    }
    fclose(f);

    if (!got_name || !got_size || !got_md5 || t->filesize <= 0 || t->npeers == 0) {
        fprintf(stderr, "tracker file %s is malformed or lists no peers\n", path);
        return -1;
    }
    if (strchr(t->filename, '/') || strstr(t->filename, "..")) {
        fprintf(stderr, "refusing unsafe filename in tracker file\n");
        return -1;
    }
    return 0;
}

/* ---------- peer selection ---------- */

/* Newest-timestamp peer that covers [s,e), isn't us, isn't dead, and hasn't
 * already failed for this segment. Returns index or -1. */
static int dl_pick_peer(dl_t *d, long s, long e, const char *tried)
{
    int best = -1;
    pthread_mutex_lock(&d->mu);
    for (int i = 0; i < d->track.npeers; i++) {
        peer_t *p = &d->track.peers[i];
        if (p->dead || tried[i]) continue;
        if (p->port == d->my_port && strcmp(p->ip, d->my_ip) == 0) continue;
        if (p->start > s || p->end < e) continue;
        if (best < 0 || p->ts > d->track.peers[best].ts) best = i;
    }
    pthread_mutex_unlock(&d->mu);
    return best;
}

/* ---------- chunk + segment download ---------- */

/* Peer-to-peer protocol (matches handle_peer / serve_peer in peer.c):
 *   request: <GET filename start end>\n     (end is exclusive, end-start <= 1024)
 *   reply:   <GET succ N>\n  followed by N raw bytes
 *            or <GET invalid>\n / <GET fail>\n
 * Returns 0 ok, -1 can't connect, -2 any other failure. */
static int dl_fetch_chunk(const char *ip, int port, const char *name,
                          long off, int len, char *buf)
{
    int fd = dl_connect(ip, port);
    if (fd < 0) return -1;

    char req[512];
    snprintf(req, sizeof req, "<GET %s %ld %ld>\n", name, off, off + len);
    if (dl_send_all(fd, req, strlen(req)) < 0) { close(fd); return -2; }

    /* Read the header line one byte at a time so no data bytes are consumed. */
    char hdr[64];
    int hn = 0;
    while (hn < (int)sizeof hdr - 1) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { close(fd); return -2; }
        if (c == '\n') break;
        hdr[hn++] = c;
    }
    hdr[hn] = '\0';

    int count = -1;
    if (sscanf(hdr, "<GET succ %d>", &count) != 1 || count != len) {
        close(fd);                         /* <GET invalid>, <GET fail>, or wrong size */
        return -2;
    }

    int got = 0;
    while (got < len) {
        ssize_t n = recv(fd, buf + got, (size_t)(len - got), 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;
        got += (int)n;
    }
    close(fd);
    return (got == len) ? 0 : -2;
}

static int fetch_segment(dl_t *d, int tid, long seg)
{
    long s = seg * DL_SEGMENT_SIZE;
    long e = s + DL_SEGMENT_SIZE;
    if (e > d->track.filesize) e = d->track.filesize;

    char tried[MAX_PEERS];
    memset(tried, 0, sizeof tried);
    char buf[DL_CHUNK_SIZE];
    long pos = s;

    while (pos < e) {
        int pi = dl_pick_peer(d, s, e, tried);
        if (pi < 0) return -1;                 /* no peer left for this segment */
        peer_t *p = &d->track.peers[pi];
        printf("[thread %d] segment %ld/%ld (bytes %ld-%ld) from %s:%d\n",
               tid, seg + 1, d->nseg, pos, e - 1, p->ip, p->port);

        while (pos < e) {
            int len = (e - pos < DL_CHUNK_SIZE) ? (int)(e - pos) : DL_CHUNK_SIZE;
            int rc = dl_fetch_chunk(p->ip, p->port, d->track.filename, pos, len, buf);
            if (rc != 0) {
                fprintf(stderr, "[thread %d] %s:%d failed at byte %ld, trying next peer\n",
                        tid, p->ip, p->port, pos);
                tried[pi] = 1;
                if (rc == -1) {                /* unreachable: skip it for everyone */
                    pthread_mutex_lock(&d->mu);
                    p->dead = 1;
                    pthread_mutex_unlock(&d->mu);
                }
                break;                         /* pick another peer, continue from `pos` */
            }
            if (dl_pwrite_all(d->fd, buf, (size_t)len, pos) < 0) {
                perror("pwrite");
                return -1;
            }
            pos += len;
        }
    }
    return 0;
}

/* ---------- updatetracker ---------- */

static void dl_send_update(dl_t *d, long end)
{
    pthread_mutex_lock(&d->upd_mu);
    if (end > d->last_reported) {
        int fd = dl_connect(d->tracker_ip, d->tracker_port);
        if (fd >= 0) {
            char msg[512], reply[256];
            snprintf(msg, sizeof msg, "<updatetracker %s 0 %ld %s %d>\n",
                     d->track.filename, end, d->my_ip, d->my_port);
            if (dl_send_all(fd, msg, strlen(msg)) == 0) {
                ssize_t n = recv(fd, reply, sizeof reply - 1, 0);
                if (n > 0) {
                    reply[n] = '\0';
                    dl_rstrip(reply);
                    printf("updatetracker (0-%ld): %s\n", end, reply);
                    if (strstr(reply, "succ")) d->last_reported = end;
                }
            }
            close(fd);
        } else {
            fprintf(stderr, "updatetracker: can't reach tracker\n");
        }
    }
    pthread_mutex_unlock(&d->upd_mu);
}

static void dl_segment_done(dl_t *d, long seg)
{
    pthread_mutex_lock(&d->mu);
    d->seg_done[seg] = 1;
    long k = 0;
    while (k < d->nseg && d->seg_done[k]) k++;       /* contiguous prefix from byte 0 */
    long end = k * DL_SEGMENT_SIZE;
    if (end > d->track.filesize) end = d->track.filesize;
    pthread_mutex_unlock(&d->mu);

    if (end > 0) dl_send_update(d, end);
}

/* ---------- download_worker + driver ---------- */

static void *download_worker(void *arg)
{
    worker_arg_t *w = arg;
    dl_t *d = w->d;
    for (;;) {
        pthread_mutex_lock(&d->mu);
        if (d->failed || d->next_seg >= d->nseg) { pthread_mutex_unlock(&d->mu); break; }
        long seg = d->next_seg++;                    /* mutually exclusive segment */
        pthread_mutex_unlock(&d->mu);

        if (fetch_segment(d, w->id, seg) != 0) {
            fprintf(stderr, "[thread %d] segment %ld could not be downloaded from any peer\n",
                    w->id, seg + 1);
            pthread_mutex_lock(&d->mu);
            d->failed = 1;
            pthread_mutex_unlock(&d->mu);
            break;
        }
        dl_segment_done(d, seg);
    }
    return NULL;
}

int download_file(const char *track_path, const char *save_dir,
                  const char *my_ip, int my_port,
                  const char *tracker_ip, int tracker_port, int nthreads)
{
    dl_t d;
    memset(&d, 0, sizeof d);
    d.my_ip = my_ip; d.my_port = my_port;
    d.tracker_ip = tracker_ip; d.tracker_port = tracker_port;

    if (dl_parse_track(track_path, &d.track) != 0) return -1;

    snprintf(d.outpath, sizeof d.outpath, "%s/%s", save_dir, d.track.filename);
    d.fd = open(d.outpath, O_CREAT | O_RDWR, 0644);
    if (d.fd < 0) { perror("open output file"); return -1; }
    if (ftruncate(d.fd, d.track.filesize) < 0) { perror("ftruncate"); close(d.fd); return -1; }

    d.nseg = (d.track.filesize + DL_SEGMENT_SIZE - 1) / DL_SEGMENT_SIZE;
    d.seg_done = calloc((size_t)d.nseg, 1);
    if (!d.seg_done) { close(d.fd); return -1; }
    pthread_mutex_init(&d.mu, NULL);
    pthread_mutex_init(&d.upd_mu, NULL);

    if (nthreads <= 0) nthreads = DL_DEFAULT_THREADS;
    if (nthreads > MAX_THREADS) nthreads = MAX_THREADS;
    if (nthreads > d.nseg) nthreads = (int)d.nseg;

    printf("Downloading %s (%ld bytes, %ld segments) with %d threads\n",
           d.track.filename, d.track.filesize, d.nseg, nthreads);

    pthread_t    tid[MAX_THREADS];
    worker_arg_t wa[MAX_THREADS];
    int started = 0;
    for (int i = 0; i < nthreads; i++) {
        wa[i].d = &d; wa[i].id = i + 1;
        if (pthread_create(&tid[i], NULL, download_worker, &wa[i]) != 0) {
            perror("pthread_create");
            pthread_mutex_lock(&d.mu); d.failed = 1; pthread_mutex_unlock(&d.mu);
            break;
        }
        started++;
    }
    for (int i = 0; i < started; i++) pthread_join(tid[i], NULL);

    int ok = !d.failed;
    for (long i = 0; ok && i < d.nseg; i++) if (!d.seg_done[i]) ok = 0;
    close(d.fd);

    if (ok) {
        struct stat st;
        char got[33];
        if (stat(d.outpath, &st) != 0 || st.st_size != d.track.filesize) {
            fprintf(stderr, "size mismatch after download\n"); ok = 0;
        } else if (dl_compute_md5(d.outpath, got) != 0) {
            fprintf(stderr, "could not compute MD5\n"); ok = 0;
        } else if (strcasecmp(got, d.track.md5) != 0) {
            fprintf(stderr, "MD5 MISMATCH: expected %s, got %s\n", d.track.md5, got); ok = 0;
        } else {
            printf("File %s download complete, MD5 verified (%s)\n", d.track.filename, got);
            remove(track_path);        /* done: drop the cached tracker file */
        }
    }

    free(d.seg_done);
    pthread_mutex_destroy(&d.mu);
    pthread_mutex_destroy(&d.upd_mu);
    return ok ? 0 : -1;
}
