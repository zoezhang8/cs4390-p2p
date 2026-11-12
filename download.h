#ifndef DOWNLOAD_H
#define DOWNLOAD_H

/* Ngoc's module: multithreaded segment download.
 *
 *  - 4 worker threads (default) take 16 KB segments in order
 *  - each segment is fetched in <=1024-byte chunks from the newest-timestamp
 *    peer whose range covers it, falling back to the next peer on failure
 *  - after each segment, `updatetracker` reports the contiguous range from
 *    byte 0 that this peer now holds
 *  - at the end the file size and MD5 are verified
 */

#define DL_SEGMENT_SIZE   16384
#define DL_CHUNK_SIZE     1024
#define DL_DEFAULT_THREADS 4

/* YOU provide this: write the 32-char lowercase hex MD5 of the file at `path`
 * into out_hex (NUL-terminated). Return 0 on success, -1 on error.
 * In peer.c: return md5_file(path, out_hex); */
int dl_compute_md5(const char *path, char out_hex[33]);

/* Download the file described by the cached tracker file `track_path`
 * into `save_dir`. my_ip/my_port identify THIS peer (used to skip ourselves
 * as a source and in updatetracker). Returns 0 on success (size + MD5 verified,
 * cached tracker file deleted), -1 on failure. */
int download_file(const char *track_path, const char *save_dir,
                  const char *my_ip, int my_port,
                  const char *tracker_ip, int tracker_port,
                  int nthreads);

#endif
