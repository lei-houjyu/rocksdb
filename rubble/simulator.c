/**
 * gcc simulator.c -o simulator -D_GNU_SOURCE
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>
#include <time.h>
#include <assert.h>
#define BUF_SIZE 17 * 1024 * 1024
 
int main(int argc, char * argv[])
{
    int fd;
    int ret;
    unsigned char *w_buf, *r_buf;
    unsigned int w_sum = 0, r_sum = 0;

    ret = posix_memalign((void **)&w_buf, 512, BUF_SIZE);
    if (ret) {
        perror("write posix_memalign failed");
        exit(1);
    }

    for (int i = 0; i < BUF_SIZE; i++) {
        w_buf[i] = i % 255;
        w_sum += (unsigned int)w_buf[i];
    }
 
    fd = open(argv[1], O_WRONLY | O_DIRECT | O_CREAT | O_DSYNC, 0755);
    if (fd < 0){
        perror("write open failed");
        exit(1);
    }

    ssize_t w_bytes = write(fd, w_buf, BUF_SIZE);

    free(w_buf);
    close(fd);

    ret = posix_memalign((void **)&r_buf, 512, BUF_SIZE);
    if (ret) {
        perror("read posix_memalign failed");
        exit(1);
    }

    fd = open(argv[1], O_RDONLY | O_DIRECT, 0755);
    if (fd < 0){
        perror("read open failed");
        exit(1);
    }

    ssize_t r_bytes = read(fd, r_buf, BUF_SIZE);

    for (int i = 0; i < BUF_SIZE; i++) {
        r_sum += (unsigned int)r_buf[i];
    }

    free(r_buf);
    close(fd);

    
    if (w_bytes == r_bytes && w_sum == r_sum) {
        printf("%s OK\n", argv[1]);
    } else {
        printf("%s Bad!\n", argv[1]);
        printf("bytes written: %ld checksum: %u\n", w_bytes, w_sum);
        printf("bytes read:    %ld checksum: %u\n", r_bytes, r_sum);
    }
}
