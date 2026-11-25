#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static atomic_int fired = 0;
static int fault_fd(int fd, char *path, size_t cap) {
    const char *arm = getenv("SPOTARB_R1327_ARM_FILE");
    const char *target = getenv("SPOTARB_R1327_DB_TARGET");
    if (!arm || !target || access(arm, F_OK) != 0) return 0;
    char link[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, path, cap - 1);
    if (n <= 0) return 0;
    path[n] = '\0';
    return strstr(path, target) != NULL;
}
static int inject(int fd) {
    char path[1024];
    if (!fault_fd(fd, path, sizeof(path))) return 0;
    int expected = 0;
    if (!atomic_compare_exchange_strong(&fired, &expected, 1)) return 0;
    const char *mode = getenv("SPOTARB_R1327_FAULT_MODE");
    int err = (mode && strcmp(mode, "ENOSPC") == 0) ? ENOSPC : EIO;
    const char *hit = getenv("SPOTARB_R1327_HIT_FILE");
    if (hit) {
        int out = (int)syscall(SYS_openat, AT_FDCWD, hit, O_WRONLY|O_CREAT|O_TRUNC, 0600);
        if (out >= 0) {
            char record[1200];
            int len = snprintf(record, sizeof(record), "mode=%s errno=%d path=%s\n", err == ENOSPC ? "ENOSPC" : "EIO", err, path);
            syscall(SYS_write, out, record, (size_t)len);
            syscall(SYS_close, out);
        }
    }
    errno = err;
    return 1;
}
ssize_t pwrite64(int fd, const void *buf, size_t count, off64_t off) {
    if (inject(fd)) return -1;
    return (ssize_t)syscall(SYS_pwrite64, fd, buf, count, off);
}
ssize_t pwrite(int fd, const void *buf, size_t count, off_t off) {
    if (inject(fd)) return -1;
    return (ssize_t)syscall(SYS_pwrite64, fd, buf, count, off);
}
ssize_t write(int fd, const void *buf, size_t count) {
    if (inject(fd)) return -1;
    return (ssize_t)syscall(SYS_write, fd, buf, count);
}
