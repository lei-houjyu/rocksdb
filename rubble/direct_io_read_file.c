/**
 * gcc direct_io_read_file.c -o direct_io_read_file -D_GNU_SOURCE
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>
#define BUF_SIZE 17 * 1024 * 1024
#define BLK_SIZE 512
 
int main(int argc, char * argv[])
{
    int fd;
    int ret;
    int io_size;
    unsigned int checksum = 0;
    unsigned char *buf;

    if (argc < 3) {
        printf("Usage: ./direct_io_read_file file_name io_size\n");
        return 0;
    }
    sscanf(argv[2], "%d", &io_size);

    ret = posix_memalign((void **)&buf, BLK_SIZE, BUF_SIZE);
    if (ret) {
        perror("posix_memalign failed");
        exit(1);
    }
 
    fd = open(argv[1], O_RDONLY | O_DIRECT, 0755);
    if (fd < 0){
        perror("open ./direct_io.data failed");
        exit(1);
    }

    while (ret < BUF_SIZE) {
        ret += read(fd, buf + ret, io_size);
    }

    if (ret < 0) {
        perror("write ./direct_io.data failed");
    }
    int wrong = 0;
    char blk_id[9];
    for (int i = 0; i < BUF_SIZE / BLK_SIZE; i++) {
        for (int j = 0; j < BLK_SIZE; j += 8) {
            int pos = i * BLK_SIZE + j;
            for (int k = 0; k < 8; k++) {
                checksum += (unsigned int)buf[pos + k] * (i + j + k);
            }
        }
    }
    printf("checksum = %u\n", checksum);

    free(buf);
    close(fd);
}
