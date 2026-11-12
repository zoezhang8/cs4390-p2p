/*
 * tracker.c - P2P tracker server.
 * Each connection gets a detached thread that serves one request and closes.
 *
 * sconfig: port, tracker folder (default "torrents"),
 *          dead-peer timeout in seconds (default 900).
 */

#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>

#define SCONFIG       "sconfig"
#define PEER_TIMEOUT  900    /* default dead-peer timeout (s) */

char tracker_folder[512] = "torrents";
long dead_timeout = PEER_TIMEOUT;

/* Serializes all tracker-file access */
pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;

/* Send <updatetracker filename status> */
static void update_reply(int sock, const char *filename, const char *status)
{
    char reply[512];
    snprintf(reply, sizeof reply, "<updatetracker %s %s>\n", filename, status);
    send_str(sock, reply);
}

/* Non-hidden file ending in ".track" */
static int is_track_file(const char *name)
{
    size_t len = strlen(name);
    return len > 6 && name[0] != '.' && strcmp(name + len - 6, ".track") == 0;
}

/* Reject empty names and paths that could leave the tracker folder */
static int bad_name(const char *name)
{
    return name[0] == '\0' || strchr(name, '/') != NULL || strstr(name, "..") != NULL;
}

/* Reply: <REP LIST X>, one <i name size md5> per tracker file, <REP LIST END> */
static void handle_list_req(int s)
{
    char line[MAXLINE], name[256], md5[64];
    char path[1024];
    long size;
    int count = 0, i = 1;
    DIR *d;
    struct dirent *e;
    FILE *fp;

    pthread_mutex_lock(&file_lock);

    /* Count first so X can be sent up front */
    if ((d = opendir(tracker_folder)) != NULL) {
        while ((e = readdir(d)) != NULL)
            if (is_track_file(e->d_name)) count++;
        closedir(d);
    }
    snprintf(line, sizeof line, "<REP LIST %d>\n", count);
    send_str(s, line);

    /* Fields come from each file's header */
    if ((d = opendir(tracker_folder)) != NULL) {
        while ((e = readdir(d)) != NULL) {
            if (!is_track_file(e->d_name)) continue;
            snprintf(path, sizeof path, "%s/%s", tracker_folder, e->d_name);
            if ((fp = fopen(path, "r")) == NULL) continue;

            name[0] = md5[0] = '\0';
            size = 0;
            while (fgets(line, sizeof line, fp) != NULL) {
                if (line[0] == '#') continue;
                if (strncmp(line, "Filename:", 9) == 0) sscanf(line + 9, " %255s", name);
                if (strncmp(line, "Filesize:", 9) == 0) sscanf(line + 9, " %ld", &size);
                if (strncmp(line, "MD5:", 4) == 0)      sscanf(line + 4, " %63s", md5);
            }
            fclose(fp);
            snprintf(line, sizeof line, "<%d %s %ld %s>\n", i++, name, size, md5);
            send_str(s, line);
        }
        closedir(d);
    }
    pthread_mutex_unlock(&file_lock);

    send_str(s, "<REP LIST END>\n");
    printf("%s LIST request served (%d tracker files)\n", ts_now(), count);
}

/* Reply: <REP GET BEGIN>, file bytes, <REP GET END md5>. The MD5 covers the bytes sent. */
static void handle_get_req(int s, const char *f)
{
    const char *notfound = "<REP GET END 0>\n";   /* sent on any failure */
    char path[1024], md5[33], end[64];
    FILE *fp;
    long size;
    unsigned char *data;

    if (bad_name(f)) {
        send_str(s, notfound);
        return;
    }
    snprintf(path, sizeof path, "%s/%s", tracker_folder, f);

    /* Read the whole file under the lock */
    pthread_mutex_lock(&file_lock);
    fp = fopen(path, "rb");
    if (fp == NULL) {
        pthread_mutex_unlock(&file_lock);
        send_str(s, notfound);
        printf("%s GET %s: not found\n", ts_now(), f);
        return;
    }
    fseek(fp, 0, SEEK_END);
    size = ftell(fp);
    rewind(fp);
    data = malloc(size > 0 ? (size_t)size : 1);
    if (data == NULL || (size > 0 && fread(data, 1, (size_t)size, fp) != (size_t)size)) {
        fclose(fp);
        pthread_mutex_unlock(&file_lock);
        free(data);
        send_str(s, notfound);
        return;
    }
    fclose(fp);
    pthread_mutex_unlock(&file_lock);

    md5_buf(data, (size_t)size, md5);

    send_str(s, "<REP GET BEGIN>\n");
    write_all(s, data, (size_t)size);
    snprintf(end, sizeof end, "<REP GET END %s>\n", md5);
    send_str(s, end);
    free(data);
    printf("%s GET %s served (%ld bytes)\n", ts_now(), f, size);
}

/* createtracker filename filesize description md5 ip port
 * Creates <filename>.track. Replies succ, ferr (already exists), or fail. */
static void handle_createtracker_req(int sock, const char *msg)
{
    char cmd[64], filename[256] = "", desc[256], md5[64], ip[64] = "", path[1024];
    long filesize = 0;
    int port = 0;
    const char *reply, *status;
    FILE *fp;

    if (sscanf(msg, "%63s %255s %ld %255s %63s %63s %d",
               cmd, filename, &filesize, desc, md5, ip, &port) != 7 ||
        bad_name(filename) || filesize < 0 || port <= 0) {
        status = "fail";
    } else {
        snprintf(path, sizeof path, "%s/%s.track", tracker_folder, filename);
        pthread_mutex_lock(&file_lock);
        fp = fopen(path, "wx");   /* fails if the file exists */
        if (fp == NULL) {
            status = (errno == EEXIST) ? "ferr" : "fail";
        } else {
            fprintf(fp, "Filename: %s\n", filename);
            fprintf(fp, "Filesize: %ld\n", filesize);
            fprintf(fp, "Description: %s\n", desc);
            fprintf(fp, "MD5: %s\n", md5);
            fprintf(fp, "#list of peers follows next\n");
            /* Seed has the whole file: ip:port:start:end:timestamp */
            fprintf(fp, "%s:%d:0:%ld:%ld\n", ip, port, filesize, (long)time(NULL));
            fclose(fp);
            status = "succ";
        }
        pthread_mutex_unlock(&file_lock);
    }
    reply = !strcmp(status, "succ") ? "<createtracker succ>\n"
          : !strcmp(status, "ferr") ? "<createtracker ferr>\n" : "<createtracker fail>\n";
    send_str(sock, reply);
    printf("%s createtracker %s from %s:%d -> %s\n",
           ts_now(), filename[0] ? filename : "?", ip[0] ? ip : "?", port, status);
}

/* updatetracker filename start_byte end_byte ip port
 * Adds or refreshes the peer and drops dead peers. Replies succ, ferr (no file), or fail. */
static void handle_updatetracker_req(int s, const char *msg)
{
    char line[MAXLINE], cmd[64], filename[256] = "", ip[64];
    char range_start[64], range_end[64];
    char path[1024], tmp[1040];
    char pip[64], prs[64], pre[64];
    int port, pport, in_peer_section = 0, replaced = 0, dropped = 0;
    long now, pts;
    FILE *fp, *out;

    if (sscanf(msg, "%63s %255s %63s %63s %63s %d",
               cmd, filename, range_start, range_end, ip, &port) != 6 || bad_name(filename)) {
        update_reply(s, filename, "fail");
        return;
    }

    /* Write changes to a temp file, then rename it over the original */
    snprintf(path, sizeof path, "%s/%s.track", tracker_folder, filename);
    snprintf(tmp, sizeof tmp, "%s.tmp", path);

    pthread_mutex_lock(&file_lock);
    if ((fp = fopen(path, "r")) == NULL) {
        pthread_mutex_unlock(&file_lock);
        update_reply(s, filename, "ferr");
        printf("%s updatetracker %s: no such tracker file\n", ts_now(), filename);
        return;
    }
    if ((out = fopen(tmp, "w")) == NULL) {
        fclose(fp);
        pthread_mutex_unlock(&file_lock);
        update_reply(s, filename, "fail");
        return;
    }

    now = (long)time(NULL);

    while (fgets(line, sizeof line, fp) != NULL) {
        if (line[0] == '#') {          /* peer list starts after this line */
            in_peer_section = 1;
            fputs(line, out);
            continue;
        }
        if (!in_peer_section) {
            fputs(line, out);
            continue;
        }
        /* Peer line: ip:port:start:end:timestamp */
        if (sscanf(line, " %63[^:]:%d:%63[^:]:%63[^:]:%ld", pip, &pport, prs, pre, &pts) == 5) {
            if (strcmp(pip, ip) == 0 && pport == port) {   /* this peer: refresh */
                if (!replaced) {
                    fprintf(out, "%s:%d:%s:%s:%ld\n", ip, port, range_start, range_end, now);
                    replaced = 1;
                }
                continue;
            }
            if (now - pts > dead_timeout) {   /* dead peer: drop */
                dropped++;
                continue;
            }
        }
        fputs(line, out);   /* keep live peers and unparsed lines */
    }
    if (!replaced)   /* new peer */
        fprintf(out, "%s:%d:%s:%s:%ld\n", ip, port, range_start, range_end, now);

    fclose(fp);
    if (fclose(out) != 0 || rename(tmp, path) != 0) {
        remove(tmp);
        pthread_mutex_unlock(&file_lock);
        update_reply(s, filename, "fail");
        return;
    }
    pthread_mutex_unlock(&file_lock);

    update_reply(s, filename, "succ");
    printf("%s updatetracker %s %s-%s from %s:%d -> succ%s\n", ts_now(), filename,
           range_start, range_end, ip, port, dropped ? " (dead peers removed)" : "");
}

/* Serve one request, then close the connection. */
static void *handle_client(void *arg)
{
    int sock = *(int *)arg;
    char msg[MAXLINE], first[64] = "", second[64] = "", fname[256] = "";
    free(arg);

    if (read_line(sock, msg, sizeof msg) > 0) {
        strip_message(msg);                        /* < > are optional */
        sscanf(msg, "%63s %63s", first, second);

        if (strcasecmp(first, "REQ") == 0 && strcasecmp(second, "LIST") == 0) {
            handle_list_req(sock);
        } else if (strcasecmp(first, "GET") == 0) {
            sscanf(msg, "%*s %255s", fname);
            handle_get_req(sock, fname);
        } else if (strcasecmp(first, "createtracker") == 0) {
            handle_createtracker_req(sock, msg);
        } else if (strcasecmp(first, "updatetracker") == 0) {
            handle_updatetracker_req(sock, msg);
        } else {
            printf("%s unknown command ignored: %s\n", ts_now(), msg);
        }
    }
    close(sock);
    return NULL;
}

int main(void)
{
    char line[MAXLINE];
    int port, lfd;

    setvbuf(stdout, NULL, _IOLBF, 0);   /* line-buffered output */
    signal(SIGPIPE, SIG_IGN);           /* don't die if a peer disconnects early */

    if (config_line(SCONFIG, 1, line, sizeof line) != 0 || (port = atoi(line)) <= 0) {
        fprintf(stderr, "%s: line 1 must be the tracker port\n", SCONFIG);
        return 1;
    }
    if (config_line(SCONFIG, 2, line, sizeof line) == 0 && line[0] != '\0')
        snprintf(tracker_folder, sizeof tracker_folder, "%.511s", line);
    if (config_line(SCONFIG, 3, line, sizeof line) == 0 && atol(line) > 0)
        dead_timeout = atol(line);

    if (mkdir(tracker_folder, 0777) != 0 && errno != EEXIST) {
        perror("mkdir");
        return 1;
    }

    if ((lfd = listen_on(port)) < 0) {
        fprintf(stderr, "Tracker cannot listen on port %d: %s\n", port, strerror(errno));
        return 1;
    }
    printf("%s Tracker ready on port %d (folder %s/, dead peer timeout %lds)\n",
           ts_now(), port, tracker_folder, dead_timeout);

    while (1) {
        int fd = accept(lfd, NULL, NULL);
        if (fd < 0) continue;

        int *arg = malloc(sizeof(int));
        if (arg == NULL) { close(fd); continue; }
        *arg = fd;

        pthread_t tid;
        if (pthread_create(&tid, NULL, handle_client, arg) != 0) {
            printf("%s cannot create worker thread\n", ts_now());
            close(fd);
            free(arg);
        } else {
            pthread_detach(tid);
        }
    }
}
