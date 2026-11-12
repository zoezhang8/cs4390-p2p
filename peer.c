#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <libgen.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define CLIENT_CFG  "clientThreadConfig.cfg"
#define SERVER_CFG  "serverThreadConfig.cfg"
#define CACHE_DIR   "cache"     /* downloaded .track files */

/* Set by read_configs() */
int  tracker_port;
char tracker_ip[64];
int  update_interval = 900;     /* seconds */
int  my_port;
char shared_dir[256];
char my_ip[64];                 /* detected, not configured */
char peer_name[64];             /* current folder name, e.g. "peer1" */

/* Copy the last non-empty line of a config file into out. Returns 0 or -1. */
static int config_last_line(const char *path, char *out, int max)
{
    char line[MAXLINE];
    int found = -1;
    for (int n = 1; config_line(path, n, line, sizeof line) == 0; n++) {
        if (line[0] != '\0') {
            snprintf(out, (size_t)max, "%s", line);
            found = 0;
        }
    }
    return found;
}

/* Read both config files; exit on error. */
static void read_configs(void)
{
    char line[MAXLINE];

    /* client cfg: tracker port, tracker IP, update interval (last line) */
    if (config_line(CLIENT_CFG, 1, line, sizeof line) != 0) {
        fprintf(stderr, "Error: cannot read %s\n", CLIENT_CFG);
        exit(1);
    }
    tracker_port = atoi(line);
    if (config_line(CLIENT_CFG, 2, tracker_ip, sizeof tracker_ip) != 0 ||
        config_last_line(CLIENT_CFG, line, sizeof line) != 0) {
        fprintf(stderr, "Error: %s needs 3 lines (tracker port, tracker IP, update interval)\n",
                CLIENT_CFG);
        exit(1);
    }
    update_interval = atoi(line);

    /* server cfg: listen port, shared folder (last line) */
    if (config_line(SERVER_CFG, 1, line, sizeof line) != 0 ||
        config_last_line(SERVER_CFG, shared_dir, sizeof shared_dir) != 0) {
        fprintf(stderr, "Error: %s needs 2 lines (listen port, shared folder)\n", SERVER_CFG);
        exit(1);
    }
    my_port = atoi(line);

    if (tracker_port <= 0 || my_port <= 0 || update_interval <= 0) {
        fprintf(stderr, "Error: ports and update interval must be positive numbers\n");
        exit(1);
    }
}

/* Get the local IP by connecting a UDP socket toward the tracker (nothing is sent). */
static void find_my_ip(void)
{
    strcpy(my_ip, "127.0.0.1");                 /* fallback */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)tracker_port);
    if (inet_pton(AF_INET, tracker_ip, &addr.sin_addr) == 1 &&
        connect(fd, (struct sockaddr *)&addr, sizeof addr) == 0) {
        struct sockaddr_in local;
        socklen_t len = sizeof local;
        if (getsockname(fd, (struct sockaddr *)&local, &len) == 0)
            inet_ntop(AF_INET, &local.sin_addr, my_ip, sizeof my_ip);
    }
    close(fd);
}

/* Send "<cmd>\n" to the tracker and return its full reply (caller frees), or NULL. */
static char *tracker_request(const char *cmd)
{
    int fd = connect_to(tracker_ip, tracker_port);
    if (fd < 0) {
        printf("%s %s: cannot connect to tracker %s:%d\n",
               ts_now(), peer_name, tracker_ip, tracker_port);
        return NULL;
    }

    char msg[MAXLINE + 4];
    snprintf(msg, sizeof msg, "<%s>\n", cmd);
    if (send_str(fd, msg) < 0) {
        printf("%s %s: failed to send request to tracker\n", ts_now(), peer_name);
        close(fd);
        return NULL;
    }

    /* Tracker closes the connection when done; grow the buffer as needed */
    size_t cap = 4096, len = 0;
    char *reply = malloc(cap);
    if (!reply) { close(fd); return NULL; }
    ssize_t n;
    while ((n = read(fd, reply + len, cap - len - 1)) > 0) {
        len += (size_t)n;
        if (len + 1 >= cap) {
            char *bigger = realloc(reply, cap * 2);
            if (!bigger) { free(reply); close(fd); return NULL; }
            reply = bigger;
            cap *= 2;
        }
    }
    reply[len] = '\0';
    close(fd);
    return reply;
}

/* Extract the tracker file from a GET reply, verify its MD5, and save it to cache/. */
static void save_tracker_file(const char *cmd, const char *reply)
{
    char track_name[256];
    if (sscanf(cmd, "GET %255s", track_name) != 1) return;

    const char *begin_tag = "<REP GET BEGIN>\n";
    const char *begin = strstr(reply, begin_tag);
    const char *end   = strstr(reply, "<REP GET END");
    if (!begin || !end || end < begin) {
        printf("%s %s: GET %s failed (bad or missing reply)\n", ts_now(), peer_name, track_name);
        return;
    }
    const char *content = begin + strlen(begin_tag);
    size_t content_len  = (size_t)(end - content);

    char expected_md5[33] = "", actual_md5[33];
    sscanf(end, "<REP GET END %32[0-9a-fA-F]", expected_md5);
    md5_buf((const unsigned char *)content, content_len, actual_md5);

    if (strcasecmp(actual_md5, expected_md5) != 0) {
        printf("%s %s: MD5 mismatch for %s (expected %s, got %s) - discarding\n",
               ts_now(), peer_name, track_name, expected_md5, actual_md5);
        return;
    }

    char path[512];
    snprintf(path, sizeof path, "%s/%s", CACHE_DIR, track_name);
    FILE *f = fopen(path, "wb");
    if (!f) { printf("%s %s: cannot write %s\n", ts_now(), peer_name, path); return; }
    fwrite(content, 1, content_len, f);
    fclose(f);
    printf("%s %s: tracker file %s received, MD5 OK, saved to %s\n",
           ts_now(), peer_name, track_name, path);
    /* Download code starts here */
}

/* share <filename> <description>: build and send createtracker for a shared file. */
static void share_file(const char *args)
{
    char fname[256], desc[512] = "";
    if (sscanf(args, "%255s %511[^\n]", fname, desc) < 1) {
        printf("Usage: share <filename> <description>\n");
        return;
    }
    if (desc[0] == '\0') strcpy(desc, "no_description");
    for (char *p = desc; *p; p++) if (*p == ' ') *p = '_';   /* spec uses _ for spaces */

    char path[512];
    snprintf(path, sizeof path, "%s/%s", shared_dir, fname);
    struct stat st;
    if (stat(path, &st) != 0) {
        printf("%s %s: %s not found in shared folder\n", ts_now(), peer_name, fname);
        return;
    }
    char md5[33];
    if (md5_file(path, md5) != 0) {
        printf("%s %s: could not compute MD5 of %s\n", ts_now(), peer_name, fname);
        return;
    }

    char cmd[MAXLINE];
    snprintf(cmd, sizeof cmd, "createtracker %s %lld %s %s %s %d",
             fname, (long long)st.st_size, desc, md5, my_ip, my_port);
    printf("%s %s: %s\n", ts_now(), peer_name, cmd);

    char *reply = tracker_request(cmd);
    if (reply) { printf("%s", reply); free(reply); }
}

/* Serve one chunk request: <GET file start end> (end exclusive, max CHUNK_SIZE bytes).
 * Replies <GET succ N> + N bytes, <GET invalid>, or <GET fail>. */
static void *handle_peer(void *arg)
{
    int fd = *(int *)arg;
    free(arg);

    char req[MAXLINE], fname[256];
    long long start, end;
    if (read_line(fd, req, sizeof req) <= 0) { close(fd); return NULL; }
    strip_message(req);
    if (sscanf(req, "GET %255s %lld %lld", fname, &start, &end) != 3) {
        send_str(fd, "<GET fail>\n");
        close(fd);
        return NULL;
    }

    long long want = end - start;
    if (start < 0 || want <= 0 || want > CHUNK_SIZE) {
        send_str(fd, "<GET invalid>\n");
        printf("%s %s: rejected request for %lld bytes of %s (limit %d)\n",
               ts_now(), peer_name, want, fname, CHUNK_SIZE);
        close(fd);
        return NULL;
    }
    if (strchr(fname, '/') || strstr(fname, "..")) {        /* stay inside shared folder */
        send_str(fd, "<GET fail>\n");
        close(fd);
        return NULL;
    }

    char path[512], data[CHUNK_SIZE];
    snprintf(path, sizeof path, "%s/%s", shared_dir, fname);
    FILE *f = fopen(path, "rb");
    size_t got = 0;
    if (f && fseek(f, start, SEEK_SET) == 0)
        got = fread(data, 1, (size_t)want, f);
    if (f) fclose(f);

    if (got == 0) {
        send_str(fd, "<GET fail>\n");
    } else {
        char header[64];
        snprintf(header, sizeof header, "<GET succ %zu>\n", got);
        send_str(fd, header);
        write_all(fd, data, got);
        printf("%s %s: sent bytes %lld to %lld of %s\n",
               ts_now(), peer_name, start, start + (long long)got, fname);
    }
    close(fd);
    return NULL;
}

/* Accept peer connections forever; one thread per connection. */
static void *server_thread(void *arg)
{
    (void)arg;
    int lfd = listen_on(my_port);
    if (lfd < 0) {
        fprintf(stderr, "%s: cannot listen on port %d\n", peer_name, my_port);
        exit(1);
    }
    while (1) {
        int *cfd = malloc(sizeof(int));
        if (!cfd) continue;
        *cfd = accept(lfd, NULL, NULL);
        if (*cfd < 0) { free(cfd); continue; }

        pthread_t t;
        if (pthread_create(&t, NULL, handle_peer, cfd) != 0) {
            close(*cfd);
            free(cfd);
            continue;
        }
        pthread_detach(t);
    }
    return NULL;
}

static void print_help(void)
{
    printf("Commands:\n"
           "  REQ LIST\n"
           "  GET filename.track\n"
           "  createtracker filename filesize description md5 ip-address port-number\n"
           "  updatetracker filename start_byte end_byte ip-address port-number\n"
           "  share <filename> <description>   (builds createtracker for you)\n"
           "  help | quit\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);    /* line-buffered output */

    /* Peer name = current folder name */
    char cwd[512];
    if (getcwd(cwd, sizeof cwd))
        snprintf(peer_name, sizeof peer_name, "%s", basename(cwd));
    else
        strcpy(peer_name, "peer");

    read_configs();
    find_my_ip();
    mkdir(shared_dir, 0755);    /* ok if they already exist */
    mkdir(CACHE_DIR, 0755);

    printf("%s %s: started at %s:%d | tracker %s:%d | shared folder '%s' | update every %ds\n",
           ts_now(), peer_name, my_ip, my_port, tracker_ip, tracker_port,
           shared_dir, update_interval);

    pthread_t srv;
    if (pthread_create(&srv, NULL, server_thread, NULL) != 0) {
        fprintf(stderr, "Cannot start server thread\n");
        return 1;
    }

    print_help();
    char line[MAXLINE];
    while (printf("%s> ", peer_name), fflush(stdout), fgets(line, sizeof line, stdin)) {
        strip_message(line);         /* also accepts "<REQ LIST>" */
        if (line[0] == '\0') continue;

        if (!strcmp(line, "quit") || !strcmp(line, "exit")) break;
        if (!strcmp(line, "help")) { print_help(); continue; }
        if (!strncmp(line, "share ", 6)) { share_file(line + 6); continue; }

        if (strcmp(line, "REQ LIST") && strncmp(line, "GET ", 4) &&
            strncmp(line, "createtracker ", 14) && strncmp(line, "updatetracker ", 14)) {
            printf("Unknown command. Type 'help'.\n");
            continue;
        }

        printf("%s %s: %s\n", ts_now(), peer_name, line);
        char *reply = tracker_request(line);
        if (!reply) continue;
        printf("%s", reply);
        if (!strncmp(line, "GET ", 4)) save_tracker_file(line, reply);
        free(reply);
    }

    printf("%s terminated\n", peer_name);
    return 0;
}