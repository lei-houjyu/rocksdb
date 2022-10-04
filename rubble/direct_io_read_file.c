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
 
int main(int argc, char * argv[])
{
    int fd;
    int ret;
    int checksum = 0;
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

//    do {
        ret = read(fd, buf, BUF_SIZE);
        //printf("%s", buf);
        if (ret < 0) {
            perror("write ./direct_io.data failed");
        }
        for (int i = 0; i < BUF_SIZE; i++) {
            checksum += (int)buf[i];
        }
//    } while (ret > 0);
    printf("checksum = %d\n", checksum);

    free(buf);
    close(fd);
}
