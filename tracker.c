/*
 * tracker.c - multithreaded tracker server for the CS4390 P2P file sharing project.
 *
 * The main thread accepts connections; each connection is handed to a new worker
 * thread (pthreads) that serves exactly one request (LIST, GET, createtracker or
 * updatetracker), sends the reply and closes the connection.
 *
 * Config file "sconfig":  line 1 = listen port
 *                         line 2 = shared directory for .track files (e.g. torrents)
 *                         line 3 = peer timeout in seconds (optional, default 900);
 *                                  entries older than this are removed as dead peers
 */
#include "common.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static char torrent_dir[256] = "torrents";
static long peer_timeout = 900;
static int  worker_count = 0;                    /* numbers worker threads in the log */
static pthread_mutex_t file_lock  = PTHREAD_MUTEX_INITIALIZER; /* guards .track files */
static pthread_mutex_t count_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct { int fd; int id; char ip[64]; } worker_arg;

/* One peer line of a tracker file: ip:port:start:end:timestamp */
typedef struct { char ip[64]; int port; long start, end, ts; } peer_entry;

/* Parsed tracker file */
typedef struct {
    char filename[256], description[512], md5[64];
    long filesize;
    int npeers;
    peer_entry peers[MAX_PEERS];
} tracker_file;

/* Reject names that could escape the torrents directory. */
static int valid_name(const char *name)
{
    return name[0] != '\0' && name[0] != '.' && strchr(name, '/') == NULL;
}

static void track_path(const char *filename, char *out, size_t max)
{
    snprintf(out, max, "%s/%s.track", torrent_dir, filename);
}

/* Parse a .track file. Lines starting with '#' are comments. Returns 0 on success. */
static int load_tracker(const char *path, tracker_file *t)
{
    FILE *f = fopen(path, "r");
    char line[MAXLINE];
    if (!f) return -1;
    memset(t, 0, sizeof *t);
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '#' || line[0] == '\0') continue;
        if (!strncmp(line, "Filename:", 9))         sscanf(line + 9, " %255s", t->filename);
        else if (!strncmp(line, "Filesize:", 9))    t->filesize = atol(line + 9);
        else if (!strncmp(line, "Description:", 12)) snprintf(t->description, sizeof t->description, "%s", line + 13);
        else if (!strncmp(line, "MD5:", 4))         sscanf(line + 4, " %63s", t->md5);
        else if (t->npeers < MAX_PEERS) {
            peer_entry *p = &t->peers[t->npeers];
            if (sscanf(line, " %63[^:]:%d:%ld:%ld:%ld", p->ip, &p->port, &p->start, &p->end, &p->ts) == 5)
                t->npeers++;
        }
    }
    fclose(f);
    return 0;
}

/* Write a tracker file in the format required by the spec. */
static int save_tracker(const char *path, const tracker_file *t)
{
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "Filename: %s\nFilesize: %ld\nDescription: %s\nMD5: %s\n#list of peers follows next\n",
            t->filename, t->filesize, t->description, t->md5);
    for (int i = 0; i < t->npeers; i++)
        fprintf(f, "%s:%d:%ld:%ld:%ld\n", t->peers[i].ip, t->peers[i].port,
                t->peers[i].start, t->peers[i].end, t->peers[i].ts);
    return fclose(f) == 0 ? 0 : -1;
}

/* LIST: <REP LIST X> / <i name size md5> ... / <REP LIST END> */
static void handle_list(int fd, int id)
{
    DIR *d;
    struct dirent *e;
    char path[512], names[256][256];
    tracker_file t;
    int n = 0;

    pthread_mutex_lock(&file_lock);
    if ((d = opendir(torrent_dir)) != NULL) {
        while ((e = readdir(d)) != NULL && n < 256) {
            size_t len = strlen(e->d_name);
            if (len > 6 && !strcmp(e->d_name + len - 6, ".track"))
                snprintf(names[n++], 256, "%s", e->d_name);
        }
        closedir(d);
    }
    char line[MAXLINE];
    snprintf(line, sizeof line, "<REP LIST %d>\n", n);
    send_str(fd, line);
    for (int i = 0; i < n; i++) {
        snprintf(path, sizeof path, "%s/%s", torrent_dir, names[i]);
        if (load_tracker(path, &t) == 0) {
            snprintf(line, sizeof line, "<%d %s %ld %s>\n", i + 1, t.filename, t.filesize, t.md5);
            send_str(fd, line);
        }
    }
    pthread_mutex_unlock(&file_lock);
    send_str(fd, "<REP LIST END>\n");
    printf("%s [worker %d] LIST request served (%d tracker files)\n", ts_now(), id, n);
}

/* GET filename.track: <REP GET BEGIN> / content / <REP GET END md5-of-content> */
static void handle_get(int fd, int id, const char *arg)
{
    char name[256], path[512], md5[33], line[128];
    if (sscanf(arg, "%255s", name) != 1 || !valid_name(name)) {
        send_str(fd, "<GET invalid>\n");
        return;
    }
    snprintf(path, sizeof path, "%s/%s", torrent_dir, name);

    pthread_mutex_lock(&file_lock);
    FILE *f = fopen(path, "rb");
    char *buf = NULL;
    long len = 0;
    if (f) {
        fseek(f, 0, SEEK_END); len = ftell(f); rewind(f);
        buf = malloc((size_t)len + 1);
        if (buf && fread(buf, 1, (size_t)len, f) != (size_t)len) { free(buf); buf = NULL; }
        fclose(f);
    }
    pthread_mutex_unlock(&file_lock);

    if (!buf) {
        send_str(fd, "<GET invalid>\n");
        printf("%s [worker %d] GET %s failed: no such tracker file\n", ts_now(), id, name);
        return;
    }
    md5_buf((unsigned char *)buf, (size_t)len, md5);
    send_str(fd, "<REP GET BEGIN>\n");
    write_all(fd, buf, (size_t)len);
    snprintf(line, sizeof line, "<REP GET END %s>\n", md5);
    send_str(fd, line);
    free(buf);
    printf("%s [worker %d] GET %s served (%ld bytes)\n", ts_now(), id, name, len);
}

/* createtracker filename filesize description md5 ip port */
static void handle_create(int fd, int id, const char *arg)
{
    tracker_file t;
    char path[512];
    memset(&t, 0, sizeof t);
    t.npeers = 1;
    peer_entry *p = &t.peers[0];
    if (sscanf(arg, "%255s %ld %511s %63s %63s %d", t.filename, &t.filesize, t.description,
               t.md5, p->ip, &p->port) != 6 || !valid_name(t.filename) || t.filesize < 0) {
        send_str(fd, "<createtracker fail>\n");
        printf("%s [worker %d] createtracker failed: malformed message\n", ts_now(), id);
        return;
    }
    p->start = 0; p->end = t.filesize; p->ts = time(NULL);
    track_path(t.filename, path, sizeof path);

    pthread_mutex_lock(&file_lock);
    struct stat st;
    const char *reply;
    if (stat(path, &st) == 0)              reply = "<createtracker ferr>\n";
    else if (save_tracker(path, &t) != 0)  reply = "<createtracker fail>\n";
    else                                   reply = "<createtracker succ>\n";
    pthread_mutex_unlock(&file_lock);

    send_str(fd, reply);
    printf("%s [worker %d] createtracker %s from %s:%d -> %.*s\n", ts_now(), id, t.filename,
           p->ip, p->port, (int)strlen(reply) - 3, reply + 1);
}

/* updatetracker filename start end ip port */
static void handle_update(int fd, int id, const char *arg)
{
    char name[256], ip[64], path[512], reply[400];
    long start, end, now = time(NULL);
    int port;
    tracker_file t;

    if (sscanf(arg, "%255s %ld %ld %63s %d", name, &start, &end, ip, &port) != 5 || !valid_name(name)) {
        send_str(fd, "<updatetracker unknown fail>\n");
        return;
    }
    track_path(name, path, sizeof path);

    pthread_mutex_lock(&file_lock);
    if (load_tracker(path, &t) != 0) {
        snprintf(reply, sizeof reply, "<updatetracker %s ferr>\n", name);
    } else {
        /* Drop dead peers (no update within peer_timeout), then update or add this peer. */
        int k = 0, found = 0;
        for (int i = 0; i < t.npeers; i++) {
            peer_entry *p = &t.peers[i];
            if (!strcmp(p->ip, ip) && p->port == port) {
                p->start = start; p->end = end; p->ts = now; found = 1;
            } else if (now - p->ts > peer_timeout) {
                printf("%s [worker %d] removing dead peer %s:%d from %s\n", ts_now(), id, p->ip, p->port, name);
                continue;
            }
            t.peers[k++] = *p;
        }
        t.npeers = k;
        if (!found && t.npeers < MAX_PEERS) {
            peer_entry *p = &t.peers[t.npeers++];
            snprintf(p->ip, sizeof p->ip, "%s", ip);
            p->port = port; p->start = start; p->end = end; p->ts = now;
            found = 1;
        }
        snprintf(reply, sizeof reply, "<updatetracker %s %s>\n", name,
                 (found && save_tracker(path, &t) == 0) ? "succ" : "fail");
    }
    pthread_mutex_unlock(&file_lock);

    send_str(fd, reply);
    printf("%s [worker %d] updatetracker %s %ld-%ld from %s:%d -> %.*s\n", ts_now(), id, name,
           start, end, ip, port, (int)strlen(reply) - 3, reply + 1);
}

/* Worker thread: serve exactly one request, then close the connection. */
static void *worker(void *argp)
{
    worker_arg *a = argp;
    char msg[MAXLINE];

    printf("%s [worker %d] started for connection from %s\n", ts_now(), a->id, a->ip);
    if (read_line(a->fd, msg, sizeof msg) > 0) {
        strip_message(msg);
        if (!strcasecmp(msg, "REQ LIST"))                 handle_list(a->fd, a->id);
        else if (!strncasecmp(msg, "GET ", 4))             handle_get(a->fd, a->id, msg + 4);
        else if (!strncasecmp(msg, "createtracker ", 14))  handle_create(a->fd, a->id, msg + 14);
        else if (!strncasecmp(msg, "updatetracker ", 14))  handle_update(a->fd, a->id, msg + 14);
        else {
            send_str(a->fd, "<invalid command>\n");
            printf("%s [worker %d] unknown command: %s\n", ts_now(), a->id, msg);
        }
    }
    close(a->fd);
    free(a);
    return NULL;
}

int main(void)
{
    char buf[256];
    int port;

    signal(SIGPIPE, SIG_IGN);                    /* a peer hanging up must not kill us */
    setvbuf(stdout, NULL, _IOLBF, 0);
    if (config_line("sconfig", 1, buf, sizeof buf) != 0 || (port = atoi(buf)) <= 0) {
        fprintf(stderr, "Cannot read port from sconfig\n");
        return 1;
    }
    if (config_line("sconfig", 2, buf, sizeof buf) == 0 && buf[0]) snprintf(torrent_dir, sizeof torrent_dir, "%s", buf);
    if (config_line("sconfig", 3, buf, sizeof buf) == 0 && atol(buf) > 0) peer_timeout = atol(buf);
    mkdir(torrent_dir, 0755);

    int lfd = listen_on(port);
    if (lfd < 0) { perror("tracker: bind/listen failed"); return 1; }
    printf("Tracker SERVER READY on port %d (shared dir: %s/, peer timeout %lds)\n", port, torrent_dir, peer_timeout);

    for (;;) {
        struct sockaddr_in cli;
        socklen_t clen = sizeof cli;
        int cfd = accept(lfd, (struct sockaddr *)&cli, &clen);
        if (cfd < 0) { perror("tracker: accept"); continue; }

        worker_arg *a = malloc(sizeof *a);
        if (!a) { close(cfd); continue; }
        a->fd = cfd;
        inet_ntop(AF_INET, &cli.sin_addr, a->ip, sizeof a->ip);
        pthread_mutex_lock(&count_lock);
        a->id = ++worker_count;
        pthread_mutex_unlock(&count_lock);

        pthread_t tid;
        if (pthread_create(&tid, NULL, worker, a) != 0) {
            perror("tracker: pthread_create");
            close(cfd); free(a);
            continue;
        }
        pthread_detach(tid);                     /* worker cleans up after itself */
    }
}
