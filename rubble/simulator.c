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
#define BLK_SIZE 512
 
int main(int argc, char * argv[])
{
    int fd;
    int ret;
    int io_size;
    unsigned char *w_buf, *r_buf;
    unsigned int w_sum = 0, r_sum = 0;

    if (argc < 3) {
        printf("Usage: ./simulator file_name io_size\n");
        return 0;
    }
    sscanf(argv[2], "%d", &io_size);

    ret = posix_memalign((void **)&w_buf, BLK_SIZE, BUF_SIZE);
    if (ret) {
        perror("write posix_memalign failed");
        exit(1);
    }

    for (int i = 0; i < BUF_SIZE / BLK_SIZE; i++) {
        for (int j = 0; j < BLK_SIZE; j += 8) {
            int pos = i * BLK_SIZE + j;
            sprintf(w_buf + pos, "blk%05d", i);
        }
    }

    for (int i = 0; i < BUF_SIZE; i++) {
        w_sum += w_buf[i] * i;
    }
 
    fd = open(argv[1], O_WRONLY | O_DIRECT | O_CREAT | O_DSYNC, 0755);
    if (fd < 0){
        perror("write open failed");
        exit(1);
    }

    // ssize_t w_bytes = write(fd, w_buf, BUF_SIZE);
    ssize_t w_bytes = 0;
    while (w_bytes < BUF_SIZE) {
        ret = write(fd, w_buf + w_bytes, io_size);
        if (ret <= 0) {
            perror("write");
        }
        w_bytes += ret;
    }

    free(w_buf);
    close(fd);

    ret = posix_memalign((void **)&r_buf, BLK_SIZE, BUF_SIZE);
    if (ret) {
        perror("read posix_memalign failed");
        exit(1);
    }

    fd = open(argv[1], O_RDONLY | O_DIRECT, 0755);
    if (fd < 0){
        perror("read open failed");
        exit(1);
    }

    // ssize_t r_bytes = read(fd, r_buf, BUF_SIZE);
    ssize_t r_bytes = 0;
    while (r_bytes < BUF_SIZE) {
        ret = read(fd, r_buf + r_bytes, io_size);
        if (ret <= 0) {
            perror("write");
        }
        r_bytes += ret;
    }

    for (int i = 0; i < BUF_SIZE; i++) {
        r_sum += (unsigned int)r_buf[i] * i;
    }

    int wrong = 0;
    char blk_id[9];
    for (int i = 0; i < BUF_SIZE / BLK_SIZE; i++) {
        sprintf(blk_id, "blk%05d", i);
        for (int j = 0; j < BLK_SIZE; j += 8) {
            int pos = i * BLK_SIZE + j;
            for (int k = 0; k < 8; k++) {
                // printf("%c", r_buf[pos + k]);
                if (blk_id[k] != r_buf[pos + k]) {
                    wrong = 1;
                }
            }
        }
        if (wrong) {
            printf("\twrong! should be %s\n", blk_id);
            wrong = 0;
        }
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
