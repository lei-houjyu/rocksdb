/**
 * gcc direct_io_write_file.c -o direct_io_write_file -D_GNU_SOURCE
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h> 
#include <string.h>
#define BUF_SIZE 4 * 1024
 
int main(int argc, char * argv[])
{
    int fd;
    int ret = 0;
    unsigned char *buf;
    ret = posix_memalign((void **)&buf, 512, BUF_SIZE);
    if (ret) {
        perror("posix_memalign failed");
        exit(1);
    }
    memset(buf, 'a', BUF_SIZE);
 
    fd = open(argv[1], O_WRONLY | O_DIRECT | O_CREAT | O_SYNC, 0755);
    if (fd < 0){
        perror("open ./direct_io.data failed");
        exit(1);
    }

    for (int i = 0; i < 17825792; i += BUF_SIZE) {
        ret += write(fd, buf, BUF_SIZE);
        if (ret != i + BUF_SIZE) {
            perror("write ./direct_io.data failed");
        }
    }
 
    free(buf);
    close(fd);
}
