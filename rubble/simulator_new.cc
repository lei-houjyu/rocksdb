#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/fcntl.h>
#include <string>
#include <iostream>
#define BUF_SIZE 17 * 1024 * 1024
#define BLK_SIZE 512

int main(int argc, char * argv[]) {
    char *data_buf;
    posix_memalign((void **)&data_buf, BLK_SIZE, BUF_SIZE);
    unsigned long checksum = 0;
    for (int i = 0; i < BUF_SIZE; i += 8) {
        for (int j = 0; j < 8; j++) {
            data_buf[i + j] = '0' + j;
            checksum += data_buf[i + j] * i * j;
        }
    }

    std::string const RMT_DISK_1 = "/mnt/remote-sst/node-2/test";
    std::string const RMT_DISK_2 = "/mnt/remote-sst/node-3/test";

    int fd1, fd2;
    int ret;
    for (int i = 0; i < 1500; i++) {
        std::string filename1 = RMT_DISK_1 + "/test-" + std::to_string(i + 1);
        std::string filename2 = RMT_DISK_2 + "/test-" + std::to_string(i + 1);
        fd1 = open(filename1.data(), O_DIRECT | O_WRONLY | O_DSYNC);
        if (fd1 < 0) {
            perror("open");
            return 1;
        }
        fd2 = open(filename2.data(), O_DIRECT | O_WRONLY | O_DSYNC);
        if (fd2 < 0) {
            perror("open");
            return 1;
        }

        ret = write(fd1, data_buf, BUF_SIZE);
        if (ret != BUF_SIZE) {
            perror("write");
            return 1;
        }
        ret = write(fd2, data_buf, BUF_SIZE);
        if (ret != BUF_SIZE) {
            perror("write");
            return 1;
        }
        close(fd1);
        close(fd2);

        char *tmp_buf;
        posix_memalign((void **)&tmp_buf, BLK_SIZE, BUF_SIZE);
        unsigned long tmp_checksum;

        fd1 = open(filename1.data(), O_DIRECT | O_RDONLY);
        if (fd1 < 0) {
            perror("open");
            return 1;
        }
        fd2 = open(filename2.data(), O_DIRECT | O_RDONLY);
        if (fd2 < 0) {
            perror("open");
            return 1;
        }

        memset(tmp_buf, 0, BUF_SIZE);
        ret = read(fd1, tmp_buf, BUF_SIZE);
        if (ret != BUF_SIZE) {
            perror("read");
            return 1;
        }
        tmp_checksum = 0;
        for (int i = 0; i < BUF_SIZE; i += 8) {
            for (int j = 0; j < 8; j++) {
                tmp_checksum += tmp_buf[i + j] * i * j;
            }
        }
        if (tmp_checksum != checksum) {
            printf("data corrupted!\n");
            return 1;
        }


        memset(tmp_buf, 0, BUF_SIZE);
        ret = read(fd2, tmp_buf, BUF_SIZE);
        if (ret != BUF_SIZE) {
            perror("read");
            return 1;
        }
        tmp_checksum = 0;
        for (int i = 0; i < BUF_SIZE; i += 8) {
            for (int j = 0; j < 8; j++) {
                tmp_checksum += tmp_buf[i + j] * i * j;
            }
        }
        if (tmp_checksum != checksum) {
            printf("data corrupted!\n");
            return 1;
        }

        close(fd1);
        close(fd2);

        std::cout << "\33[2K\rfile " << i + 1 << " passed" << std::flush;
    }
    printf("\n");
}