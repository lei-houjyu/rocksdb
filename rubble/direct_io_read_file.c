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
#define BUF_SIZE 4096
 
int main(int argc, char * argv[])
{
    int fd;
    int ret;
    unsigned char *buf;
    ret = posix_memalign((void **)&buf, 512, BUF_SIZE);
    if (ret) {
        perror("posix_memalign failed");
        exit(1);
    }
 
    fd = open(argv[1], O_RDONLY | O_DIRECT, 0755);
    if (fd < 0){
        perror("open ./direct_io.data failed");
        exit(1);
    }

    do {
        ret = read(fd, buf, BUF_SIZE);
        printf("%s", buf);
        if (ret < 0) {
            perror("write ./direct_io.data failed");
        }
    } while (ret > 0);
 
    free(buf);
    close(fd);
}
